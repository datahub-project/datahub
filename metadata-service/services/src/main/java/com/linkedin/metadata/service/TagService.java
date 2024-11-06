package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.patch.builder.EditableSchemaMetadataPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.GlobalTagsPatchBuilder;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.net.URISyntaxException;
import java.util.ArrayList;
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
