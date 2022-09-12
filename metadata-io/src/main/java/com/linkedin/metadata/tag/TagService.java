package com.linkedin.metadata.tag;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.utils.GenericRecordUtils;
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

import static com.linkedin.metadata.search.utils.AspectUtils.*;


@Slf4j
public class TagService {

  private final EntityClient entityClient;
  private final Authentication systemAuthentication;

  public TagService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    this.entityClient = Objects.requireNonNull(entityClient);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
  }

  public void batchAddTags(@Nonnull List<Urn> tagUrns, @Nonnull List<ResourceReference> resources) {
    log.debug("Batch adding Tags to entities. tags: {}, resources: {}", resources, tagUrns);
    try {
      addTagsToResources(tagUrns, resources);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add Tags %s to resources with urns %s!",
          tagUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  public void batchRemoveTags(@Nonnull List<Urn> tagUrns, @Nonnull List<ResourceReference> resources) {
    log.debug("Batch adding Tags to entities. tags: {}, resources: {}", resources, tagUrns);
    try {
      removeTagsFromResources(tagUrns, resources);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add Tags %s to resources with urns %s!",
          tagUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  public void addTagsToResources(
      List<com.linkedin.common.urn.Urn> tagUrns,
      List<ResourceReference> resources
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference entity : resources) {
      MetadataChangeProposal proposal = buildAddTagsProposal(tagUrns, entity);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes);
  }

  public void removeTagsFromResources(
      List<Urn> tags,
      List<ResourceReference> resources
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      MetadataChangeProposal proposal = buildRemoveTagsProposal(tags, resource);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes);
  }

  @Nullable
  private MetadataChangeProposal buildAddTagsProposal(
      List<com.linkedin.common.urn.Urn> tagUrns,
      ResourceReference resource
  ) throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding tags to a top-level entity
      return buildAddTagsToEntityProposal(tagUrns, resource);
    } else {
      // Case 2: Adding tags to subresource (e.g. schema fields)
      return buildAddTagsToSubResourceProposal(tagUrns, resource);
    }
  }

  @Nullable
  private MetadataChangeProposal buildRemoveTagsProposal(
      List<Urn> tagUrns,
      ResourceReference resource
  ) throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding tags to a top-level entity
      return buildRemoveTagsToEntityProposal(tagUrns, resource);
    } else {
      // Case 2: Adding tags to subresource (e.g. schema fields)
      return buildRemoveTagsToSubResourceProposal(tagUrns, resource);
    }
  }

  @Nullable
  private MetadataChangeProposal buildAddTagsToEntityProposal(
      List<com.linkedin.common.urn.Urn> tagUrns,
      ResourceReference resource
  ) throws URISyntaxException {
    com.linkedin.common.GlobalTags tags =
        getGlobalTagsAspect(
            resource.getUrn(),
            new GlobalTags());

    if (tags == null) {
      return null;
    }

    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    addTagsIfNotExists(tags, tagUrns);
    return buildMetadataChangeProposal(resource.getUrn(), Constants.GLOBAL_TAGS_ASPECT_NAME, tags);
  }

  @Nullable
  private MetadataChangeProposal buildRemoveTagsToEntityProposal(
      List<Urn> tagUrns,
      ResourceReference resource
  ) {
    com.linkedin.common.GlobalTags tags = getGlobalTagsAspect(
        resource.getUrn(),
        new GlobalTags());

    if (tags == null) {
      return null;
    }

    if (!tags.hasTags()) {
      tags.setTags(new TagAssociationArray());
    }
    removeTagsIfExists(tags, tagUrns);
    return buildMetadataChangeProposal(
        resource.getUrn(),
        Constants.GLOBAL_TAGS_ASPECT_NAME, tags
    );
  }

  @Nullable
  private MetadataChangeProposal buildRemoveTagsToSubResourceProposal(
      List<Urn> tagUrns,
      ResourceReference resource
  ) {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        getEditableSchemaMetadataAspect(
            resource.getUrn(),
            new EditableSchemaMetadata());

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

  @Nullable
  private MetadataChangeProposal buildAddTagsToSubResourceProposal(
      final List<Urn> tagUrns,
      final ResourceReference resource
  ) throws URISyntaxException {
    com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
        getEditableSchemaMetadataAspect(
            resource.getUrn(),
            new EditableSchemaMetadata());

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
  private GlobalTags getGlobalTagsAspect(@Nonnull Urn entityUrn, @Nonnull GlobalTags defaultValue) {
    try {
      Aspect aspect = getLatestAspect(
          entityUrn,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          this.entityClient,
          this.systemAuthentication
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
  private EditableSchemaMetadata getEditableSchemaMetadataAspect(@Nonnull Urn entityUrn, @Nonnull EditableSchemaMetadata defaultValue) {
    try {
      Aspect aspect = getLatestAspect(
          entityUrn,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          this.entityClient,
          this.systemAuthentication
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

  @Nonnull
  public static MetadataChangeProposal buildMetadataChangeProposal(
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  private void ingestChangeProposals(@Nonnull List<MetadataChangeProposal> changes) throws Exception {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      this.entityClient.ingestProposal(change, this.systemAuthentication);
    }
  }
}
