package com.linkedin.metadata.service;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import  com.linkedin.entity.client.EntityClient;
import com.datahub.authentication.Authentication;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.entity.AspectUtils.*;


@Slf4j
public class TagService {

  private final EntityClient entityClient;
  private final Authentication systemAuthentication;

  public TagService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    this.entityClient = Objects.requireNonNull(entityClient);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
  }

  /**
   * Batch adds multiple tags for a set of resources.
   *
   * @param tagUrns the urns of the tags to add
   * @param resources references to the resources to change
   */
  public void batchAddTags(@Nonnull List<Urn> tagUrns, @Nonnull List<ResourceReference> resources) {
    batchAddTags(tagUrns, resources, this.systemAuthentication);
  }


  /**
   * Batch adds multiple tags for a set of resources.
   *
   * @param tagUrns the urns of the tags to add
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   *
   */
  public void batchAddTags(@Nonnull List<Urn> tagUrns, @Nonnull List<ResourceReference> resources, @Nonnull Authentication authentication) {
    log.debug("Batch adding Tags to entities. tags: {}, resources: {}", resources, tagUrns);
    try {
      addTagsToResources(tagUrns, resources, authentication);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add Tags %s to resources with urns %s!",
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
   *
   */
  public void batchRemoveTags(@Nonnull List<Urn> tagUrns, @Nonnull List<ResourceReference> resources) {
    batchRemoveTags(tagUrns, resources, this.systemAuthentication);
  }

  /**
   * Batch removes multiple tags for a set of resources.
   *
   * @param tagUrns the urns of the tags to remove
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   *
   */
  public void batchRemoveTags(@Nonnull List<Urn> tagUrns, @Nonnull List<ResourceReference> resources, @Nonnull Authentication authentication) {
    log.debug("Batch adding Tags to entities. tags: {}, resources: {}", resources, tagUrns);
    try {
      removeTagsFromResources(tagUrns, resources, authentication);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add Tags %s to resources with urns %s!",
          tagUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  private void addTagsToResources(
      List<com.linkedin.common.urn.Urn> tagUrns,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference entity : resources) {
      MetadataChangeProposal proposal = buildAddTagsProposal(tagUrns, entity, authentication);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  private void removeTagsFromResources(
      List<Urn> tags,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      MetadataChangeProposal proposal = buildRemoveTagsProposal(tags, resource, authentication);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  @VisibleForTesting
  @Nullable
  MetadataChangeProposal buildAddTagsProposal(
      List<com.linkedin.common.urn.Urn> tagUrns,
      ResourceReference resource,
      Authentication authentication
  ) throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding tags to a top-level entity
      return buildAddTagsToEntityProposal(tagUrns, resource, authentication);
    } else {
      // Case 2: Adding tags to subresource (e.g. schema fields)
      return buildAddTagsToSubResourceProposal(tagUrns, resource, authentication);
    }
  }

  @VisibleForTesting
  @Nullable
  MetadataChangeProposal buildRemoveTagsProposal(
      List<Urn> tagUrns,
      ResourceReference resource,
      Authentication authentication
  ) throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding tags to a top-level entity
      return buildRemoveTagsToEntityProposal(tagUrns, resource, authentication);
    } else {
      // Case 2: Adding tags to subresource (e.g. schema fields)
      return buildRemoveTagsToSubResourceProposal(tagUrns, resource, authentication);
    }
  }

  @VisibleForTesting
  @Nullable
  MetadataChangeProposal buildAddTagsToEntityProposal(
      List<com.linkedin.common.urn.Urn> tagUrns,
      ResourceReference resource,
      Authentication authentication
  ) throws URISyntaxException {
    com.linkedin.common.GlobalTags tags =
        getGlobalTagsAspect(
          resource.getUrn(),
          new GlobalTags(),
          authentication);

    if (tags == null) {
      return null;
    }

    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    addTagsIfNotExists(tags, tagUrns);
    return buildMetadataChangeProposal(resource.getUrn(), Constants.GLOBAL_TAGS_ASPECT_NAME, tags);
  }

  @VisibleForTesting
  @Nullable
  MetadataChangeProposal buildRemoveTagsToEntityProposal(
      List<Urn> tagUrns,
      ResourceReference resource,
      Authentication authentication
  ) {
    com.linkedin.common.GlobalTags tags = getGlobalTagsAspect(
      resource.getUrn(),
      new GlobalTags(),
      authentication);

    if (tags == null) {
      return null;
    }

    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    removeTagsIfExists(tags, tagUrns);
    return buildMetadataChangeProposal(
        resource.getUrn(),
        Constants.GLOBAL_TAGS_ASPECT_NAME,
        tags
    );
  }

  @VisibleForTesting
  @Nullable
  MetadataChangeProposal buildRemoveTagsToSubResourceProposal(
      List<Urn> tagUrns,
      ResourceReference resource,
      @Nonnull Authentication authentication
  ) {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        getEditableSchemaMetadataAspect(
            resource.getUrn(),
            new EditableSchemaMetadata(),
            authentication);

    if (editableSchemaMetadata == null) {
      return null;
    }

    EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

    if (!editableFieldInfo.hasGlobalTags()) {
      editableFieldInfo.setGlobalTags(new GlobalTags());
    }
    removeTagsIfExists(editableFieldInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposal(resource.getUrn(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME, editableSchemaMetadata);
  }

  @VisibleForTesting
  @Nullable
  MetadataChangeProposal buildAddTagsToSubResourceProposal(
      final List<Urn> tagUrns,
      final ResourceReference resource,
      final Authentication authentication
  ) throws URISyntaxException {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        getEditableSchemaMetadataAspect(
            resource.getUrn(),
            new EditableSchemaMetadata(),
            authentication);

    if (editableSchemaMetadata == null) {
      return null;
    }

    EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

    if (!editableFieldInfo.hasGlobalTags()) {
      editableFieldInfo.setGlobalTags(new GlobalTags());
    }

    addTagsIfNotExists(editableFieldInfo.getGlobalTags(), tagUrns);
    return buildMetadataChangeProposal(
        resource.getUrn(),
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        editableSchemaMetadata);
  }

  private void addTagsIfNotExists(GlobalTags tags, List<Urn> tagUrns) throws URISyntaxException {
    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }

    TagAssociationArray tagAssociationArray = tags.getTags();

    List<Urn> tagsToAdd = new ArrayList<>();
    for (Urn tagUrn : tagUrns) {
      if (tagAssociationArray.stream().anyMatch(association -> association.getTag().equals(tagUrn))) {
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

  @Nullable
  private GlobalTags getGlobalTagsAspect(
      @Nonnull Urn entityUrn,
      @Nonnull GlobalTags defaultValue,
      @Nonnull Authentication authentication) {
    try {
      Aspect aspect = getLatestAspect(
          entityUrn,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          this.entityClient,
          authentication
      );

      if (aspect == null) {
        return defaultValue;
      }
      return new GlobalTags(aspect.data());
    } catch (Exception e) {
      log.error(
          "Error retrieving global tags for entity. Entity: {} aspect: {}",
          entityUrn,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          e);
      return null;
    }
  }

  @Nullable
  private EditableSchemaMetadata getEditableSchemaMetadataAspect(
      @Nonnull Urn entityUrn,
      @Nonnull EditableSchemaMetadata defaultValue,
      @Nonnull Authentication authentication) {
    try {
      Aspect aspect = getLatestAspect(
          entityUrn,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          this.entityClient,
          authentication
      );

      if (aspect == null) {
        return defaultValue;
      }

      return new EditableSchemaMetadata(aspect.data());
    } catch (Exception e) {
      log.error(
          "Error retrieving editable schema metadata for entity. Entity: {} aspect: {}.",
          entityUrn,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          e
      );
      return null;
    }
  }

  private static EditableSchemaFieldInfo getFieldInfoFromSchema(
      EditableSchemaMetadata editableSchemaMetadata,
      String fieldPath
  ) {
    if (!editableSchemaMetadata.hasEditableSchemaFieldInfo()) {
      editableSchemaMetadata.setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray());
    }
    EditableSchemaFieldInfoArray editableSchemaMetadataArray =
        editableSchemaMetadata.getEditableSchemaFieldInfo();
    Optional<EditableSchemaFieldInfo> fieldMetadata = editableSchemaMetadataArray
        .stream()
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

  private void ingestChangeProposals(@Nonnull List<MetadataChangeProposal> changes, @Nonnull Authentication authentication) throws Exception {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      this.entityClient.ingestProposal(change, authentication);
    }
  }
}
