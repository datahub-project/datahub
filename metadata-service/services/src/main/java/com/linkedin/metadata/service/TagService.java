package com.linkedin.metadata.service;

import static com.linkedin.metadata.entity.AspectUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TagService extends BaseService {

  public TagService(@Nonnull SystemEntityClient entityClient) {
    super(entityClient);
  }

  /**
   * Batch adds multiple tags for a set of resources.
   *
   * @param tagUrns the urns of the tags to add
   * @param resources references to the resources to change
   */
  public void batchAddTags(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> tagUrns,
      @Nonnull List<ResourceReference> resources) {
    log.debug("Batch adding Tags to entities. tags: {}, resources: {}", resources, tagUrns);
    try {
      addTagsToResources(opContext, tagUrns, resources);
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
   */
  public void batchRemoveTags(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> tagUrns,
      @Nonnull List<ResourceReference> resources) {
    log.debug("Batch adding Tags to entities. tags: {}, resources: {}", resources, tagUrns);
    try {
      removeTagsFromResources(opContext, tagUrns, resources);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add Tags %s to resources with urns %s!",
              tagUrns,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  private void addTagsToResources(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> tagUrns,
      List<ResourceReference> resources)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildAddTagsProposals(opContext, tagUrns, resources);
    ingestChangeProposals(opContext, changes);
  }

  private void removeTagsFromResources(
      @Nonnull OperationContext opContext, List<Urn> tags, List<ResourceReference> resources)
      throws Exception {
    final List<MetadataChangeProposal> changes =
        buildRemoveTagsProposals(opContext, tags, resources);
    ingestChangeProposals(opContext, changes);
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddTagsProposals(
      @Nonnull OperationContext opContext, List<Urn> tagUrns, List<ResourceReference> resources)
      throws URISyntaxException {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResource() == null || resource.getSubResource().equals(""))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals =
        buildAddTagsToEntityProposals(opContext, tagUrns, entityRefs);

    final List<ResourceReference> schemaFieldRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResourceType() != null
                        && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> schemaFieldProposals =
        buildAddTagsToSubResourceProposals(opContext, tagUrns, schemaFieldRefs);

    changes.addAll(entityProposals);
    changes.addAll(schemaFieldProposals);

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveTagsProposals(
      @Nonnull OperationContext opContext, List<Urn> tagUrns, List<ResourceReference> resources) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResource() == null || resource.getSubResource().equals(""))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals =
        buildRemoveTagsToEntityProposals(opContext, tagUrns, entityRefs);

    final List<ResourceReference> schemaFieldRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResourceType() != null
                        && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> schemaFieldProposals =
        buildRemoveTagsToSubResourceProposals(opContext, tagUrns, schemaFieldRefs);

    changes.addAll(entityProposals);
    changes.addAll(schemaFieldProposals);

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddTagsToEntityProposals(
      @Nonnull OperationContext opContext, List<Urn> tagUrns, List<ResourceReference> resources)
      throws URISyntaxException {
    final Map<Urn, GlobalTags> tagsAspects =
        getTagsAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new GlobalTags());

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      GlobalTags globalTags = tagsAspects.get(resource.getUrn());
      if (globalTags == null) {
        continue; // Something went wrong.
      }
      if (!globalTags.hasTags()) {
        globalTags.setTags(new TagAssociationArray());
      }
      addTagsIfNotExists(globalTags, tagUrns);
      MetadataChangeProposal proposal =
          buildMetadataChangeProposal(
              resource.getUrn(), Constants.GLOBAL_TAGS_ASPECT_NAME, globalTags);
      changes.add(proposal);
    }
    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddTagsToSubResourceProposals(
      @Nonnull OperationContext opContext,
      final List<Urn> tagUrns,
      final List<ResourceReference> resources)
      throws URISyntaxException {

    final Map<Urn, EditableSchemaMetadata> editableSchemaMetadataAspects =
        getEditableSchemaMetadataAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new EditableSchemaMetadata());

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {

      EditableSchemaMetadata editableSchemaMetadata =
          editableSchemaMetadataAspects.get(resource.getUrn());
      if (editableSchemaMetadata == null) {
        continue; // Something went wrong.
      }

      EditableSchemaFieldInfo editableFieldInfo =
          getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      addTagsIfNotExists(editableFieldInfo.getGlobalTags(), tagUrns);
      changes.add(
          buildMetadataChangeProposal(
              resource.getUrn(),
              Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
              editableSchemaMetadata));
    }

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveTagsToEntityProposals(
      @Nonnull OperationContext opContext, List<Urn> tagUrns, List<ResourceReference> resources) {
    final Map<Urn, GlobalTags> tagsAspects =
        getTagsAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new GlobalTags());

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      GlobalTags globalTags = tagsAspects.get(resource.getUrn());
      if (globalTags == null) {
        continue; // Something went wrong.
      }
      if (!globalTags.hasTags()) {
        globalTags.setTags(new TagAssociationArray());
      }
      removeTagsIfExists(globalTags, tagUrns);
      MetadataChangeProposal proposal =
          buildMetadataChangeProposal(
              resource.getUrn(), Constants.GLOBAL_TAGS_ASPECT_NAME, globalTags);

      changes.add(proposal);
    }
    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveTagsToSubResourceProposals(
      @Nonnull OperationContext opContext, List<Urn> tagUrns, List<ResourceReference> resources) {
    final Map<Urn, EditableSchemaMetadata> editableSchemaMetadataAspects =
        getEditableSchemaMetadataAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new EditableSchemaMetadata());

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {

      EditableSchemaMetadata editableSchemaMetadata =
          editableSchemaMetadataAspects.get(resource.getUrn());
      if (editableSchemaMetadata == null) {
        continue; // Something went wrong.
      }

      EditableSchemaFieldInfo editableFieldInfo =
          getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }
      removeTagsIfExists(editableFieldInfo.getGlobalTags(), tagUrns);
      changes.add(
          buildMetadataChangeProposal(
              resource.getUrn(),
              Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
              editableSchemaMetadata));
    }

    return changes;
  }

  private void addTagsIfNotExists(GlobalTags tags, List<Urn> tagUrns) throws URISyntaxException {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }

    TagAssociationArray tagAssociationArray = tags.getTags();

    List<Urn> tagsToAdd = new ArrayList<>();
    for (Urn tagUrn : tagUrns) {
      if (tagAssociationArray.stream()
          .anyMatch(association -> association.getTag().equals(tagUrn))) {
        continue;
      }
      tagsToAdd.add(tagUrn);
    }

    // Check for no tags to add
    if (tagsToAdd.size() == 0) {
      return;
    }

    for (Urn tagUrn : tagsToAdd) {
      TagAssociation newAssociation = new TagAssociation();
      newAssociation.setTag(TagUrn.createFromUrn(tagUrn));
      tagAssociationArray.add(newAssociation);
    }
  }

  private static TagAssociationArray removeTagsIfExists(GlobalTags tags, List<Urn> tagUrns) {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    TagAssociationArray tagAssociationArray = tags.getTags();
    for (Urn tagUrn : tagUrns) {
      tagAssociationArray.removeIf(association -> association.getTag().equals(tagUrn));
    }
    return tagAssociationArray;
  }

  private static EditableSchemaFieldInfo getFieldInfoFromSchema(
      EditableSchemaMetadata editableSchemaMetadata, String fieldPath) {
    if (!editableSchemaMetadata.hasEditableSchemaFieldInfo()) {
      editableSchemaMetadata.setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray());
    }
    EditableSchemaFieldInfoArray editableSchemaMetadataArray =
        editableSchemaMetadata.getEditableSchemaFieldInfo();
    Optional<EditableSchemaFieldInfo> fieldMetadata =
        editableSchemaMetadataArray.stream()
            .filter(fieldInfo -> fieldInfo.getFieldPath().equals(fieldPath))
            .findFirst();

    if (fieldMetadata.isPresent()) {
      return fieldMetadata.get();
    } else {
      EditableSchemaFieldInfo newFieldInfo = new EditableSchemaFieldInfo();
      newFieldInfo.setFieldPath(fieldPath);
      editableSchemaMetadataArray.add(newFieldInfo);
      return newFieldInfo;
    }
  }
}
