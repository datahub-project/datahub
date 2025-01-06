package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.patch.builder.EditableSchemaMetadataPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.GlobalTagsPatchBuilder;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.SchemaField;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class TagService extends BaseService {

  private final boolean _isAsync;

  public TagService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      final boolean isAsync) {
    super(entityClient, openApiClient, objectMapper);
    _isAsync = isAsync;
  }

  public TagService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    this(entityClient, openApiClient, objectMapper, false);
  }

  /**
   * Batch adds multiple tags for a set of resources.
   *
   * @param tagUrns the urns of the tags to add
   * @param resources references to the resources to change
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public List<MetadataChangeProposal> batchAddTags(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> tagUrns,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource) {
    log.debug("Batch adding Tags to entities. tags: {}, resources: {}", resources, tagUrns);
    try {
      return addTagsToResources(opContext, tagUrns, resources, appSource);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Tags %s to resources with urns %s!",
              tagUrns,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Batch removes multiple tags for a set of resources.
   *
   * @param tagUrns the urns of the tags to remove
   * @param resources references to the resources to change
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public List<MetadataChangeProposal> batchRemoveTags(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> tagUrns,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource) {
    log.debug("Batch adding Tags to entities. tags: {}, resources: {}", resources, tagUrns);
    try {
      return removeTagsFromResources(opContext, tagUrns, resources, appSource);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Tags %s to resources with urns %s!",
              tagUrns,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  /**
   * Retrieve all asset level tags for a given asset.
   *
   * @param opContext the operation context
   * @param entityUrn the entity to retrieve tags for.
   * @return the tag associations associated with the entity.
   */
  public List<TagAssociation> getEntityTags(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final GlobalTags maybeGlobalTags = getGlobalTags(opContext, entityUrn);
    if (maybeGlobalTags != null) {
      return maybeGlobalTags.getTags();
    }
    return Collections.emptyList();
  }

  /**
   * Retrieve all schema-field level tags for a given asset.
   *
   * @param opContext the operation context
   * @param entityUrn the entity to retrieve tags for
   * @return the tag associations associated with the schema field
   */
  public List<TagAssociation> getSchemaFieldTags(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final List<TagAssociation> editableSchemaFieldTags =
        getEditableSchemaFieldTags(opContext, entityUrn, fieldPath);
    final List<TagAssociation> schemaFieldTags =
        getNonEditableSchemaFieldTags(opContext, entityUrn, fieldPath);

    List<TagAssociation> result = new ArrayList<>();
    result.addAll(editableSchemaFieldTags);
    result.addAll(schemaFieldTags);

    return result;
  }

  private List<TagAssociation> getEditableSchemaFieldTags(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final EditableSchemaFieldInfo maybeEditableSchemaField =
        getEditableSchemaField(opContext, entityUrn, fieldPath);
    if (maybeEditableSchemaField != null && maybeEditableSchemaField.hasGlobalTags()) {
      return maybeEditableSchemaField.getGlobalTags().getTags();
    }
    return Collections.emptyList();
  }

  private List<TagAssociation> getNonEditableSchemaFieldTags(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final SchemaField maybeSchemaField = getSchemaField(opContext, entityUrn, fieldPath);
    if (maybeSchemaField != null && maybeSchemaField.hasGlobalTags()) {
      return maybeSchemaField.getGlobalTags().getTags();
    }
    return Collections.emptyList();
  }

  @Nullable
  private GlobalTags getGlobalTags(@Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getGlobalTagsEntityResponse(opContext, entityUrn);
    if (response != null && response.getAspects().containsKey(GLOBAL_TAGS_ASPECT_NAME)) {
      return new GlobalTags(response.getAspects().get(GLOBAL_TAGS_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  private EntityResponse getGlobalTagsEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(GLOBAL_TAGS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve tags for entity with urn %s", entityUrn), e);
    }
  }

  private List<MetadataChangeProposal> addTagsToResources(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> tagUrns,
      List<ResourceReference> resources,
      @Nullable String appSource)
      throws Exception {
    final List<MetadataChangeProposal> changes = patchAddGlobalTags(tagUrns, resources);
    if (StringUtils.isNotBlank(appSource)) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
    return changes;
  }

  private List<MetadataChangeProposal> removeTagsFromResources(
      @Nonnull OperationContext opContext,
      List<Urn> tags,
      List<ResourceReference> resources,
      @Nullable String appSource)
      throws Exception {
    final List<MetadataChangeProposal> changes = patchRemoveGlobalTags(tags, resources);
    if (StringUtils.isNotBlank(appSource)) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
    return changes;
  }

  private static List<MetadataChangeProposal> patchAddGlobalTags(
      List<Urn> tagUrns, List<ResourceReference> resources) throws URISyntaxException {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResource() == null || resource.getSubResource().equals(""))
            .collect(Collectors.toList());

    for (ResourceReference resource : entityRefs) {
      GlobalTagsPatchBuilder patchBuilder = new GlobalTagsPatchBuilder().urn(resource.getUrn());
      for (Urn tagUrn : tagUrns) {
        patchBuilder.addTag(TagUrn.createFromUrn(tagUrn), null);
      }
      changes.add(patchBuilder.build());
    }

    final List<ResourceReference> schemaFieldRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResourceType() != null
                        && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
            .collect(Collectors.toList());

    for (ResourceReference resource : schemaFieldRefs) {
      EditableSchemaMetadataPatchBuilder patchBuilder =
          new EditableSchemaMetadataPatchBuilder().urn(resource.getUrn());
      for (Urn tagUrn : tagUrns) {
        TagAssociation newAssociation = new TagAssociation();
        newAssociation.setTag(TagUrn.createFromUrn(tagUrn));
        patchBuilder.addTag(newAssociation, resource.getSubResource());
      }
      changes.add(patchBuilder.build());
    }

    return changes;
  }

  private static List<MetadataChangeProposal> patchRemoveGlobalTags(
      List<Urn> tagUrns, List<ResourceReference> resources) throws URISyntaxException {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResource() == null || resource.getSubResource().equals(""))
            .collect(Collectors.toList());

    for (ResourceReference resource : entityRefs) {
      GlobalTagsPatchBuilder patchBuilder = new GlobalTagsPatchBuilder().urn(resource.getUrn());
      for (Urn tagUrn : tagUrns) {
        patchBuilder.removeTag(TagUrn.createFromUrn(tagUrn));
      }
      changes.add(patchBuilder.build());
    }

    final List<ResourceReference> schemaFieldRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResourceType() != null
                        && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
            .collect(Collectors.toList());

    for (ResourceReference resource : schemaFieldRefs) {
      EditableSchemaMetadataPatchBuilder patchBuilder =
          new EditableSchemaMetadataPatchBuilder().urn(resource.getUrn());
      for (Urn tagUrn : tagUrns) {
        patchBuilder.removeTag(TagUrn.createFromUrn(tagUrn), resource.getSubResource());
      }
      changes.add(patchBuilder.build());
    }

    return changes;
  }
}
