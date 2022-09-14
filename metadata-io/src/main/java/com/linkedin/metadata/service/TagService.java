package com.linkedin.metadata.service;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import  com.linkedin.entity.client.EntityClient;
import com.datahub.authentication.Authentication;
import javax.annotation.Nonnull;
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
    final List<MetadataChangeProposal> changes = buildAddTagsProposals(tagUrns, resources, authentication);
    ingestChangeProposals(changes, authentication);
  }

  private void removeTagsFromResources(
      List<Urn> tags,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = buildRemoveTagsProposals(tags, resources, authentication);
    ingestChangeProposals(changes, authentication);
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddTagsProposals(
      List<Urn> tagUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws URISyntaxException {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs = resources.stream()
        .filter(resource -> resource.getSubResource() == null || resource.getSubResource().equals(""))
        .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals = buildAddTagsToEntityProposals(tagUrns, entityRefs, authentication);

    final List<ResourceReference> schemaFieldRefs = resources.stream()
        .filter(resource -> resource.getSubResourceType() != null && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
        .collect(Collectors.toList());
    final List<MetadataChangeProposal> schemaFieldProposals = buildAddTagsToSubResourceProposals(tagUrns, schemaFieldRefs, authentication);

    changes.addAll(entityProposals);
    changes.addAll(schemaFieldProposals);

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveTagsProposals(
      List<Urn> tagUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) {
    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs = resources.stream()
        .filter(resource -> resource.getSubResource() == null || resource.getSubResource().equals(""))
        .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals = buildRemoveTagsToEntityProposals(tagUrns, entityRefs, authentication);

    final List<ResourceReference> schemaFieldRefs = resources.stream()
        .filter(resource -> resource.getSubResourceType() != null && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
        .collect(Collectors.toList());
    final List<MetadataChangeProposal> schemaFieldProposals = buildRemoveTagsToSubResourceProposals(tagUrns, schemaFieldRefs, authentication);

    changes.addAll(entityProposals);
    changes.addAll(schemaFieldProposals);

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddTagsToEntityProposals(
      List<Urn> tagUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws URISyntaxException {
    final Map<Urn, GlobalTags> tagsAspects = getTagsAspects(
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
        new GlobalTags(),
        authentication
    );

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
      MetadataChangeProposal proposal = buildMetadataChangeProposal(
          resource.getUrn(),
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          globalTags
      );
      changes.add(proposal);
    }
    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddTagsToSubResourceProposals(
      final List<Urn> tagUrns,
      final List<ResourceReference> resources,
      final Authentication authentication
  ) throws URISyntaxException {

    final Map<Urn, EditableSchemaMetadata> editableSchemaMetadataAspects = getEditableSchemaMetadataAspects(
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
        new EditableSchemaMetadata(),
        authentication
    );

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {

      EditableSchemaMetadata editableSchemaMetadata = editableSchemaMetadataAspects.get(resource.getUrn());
      if (editableSchemaMetadata == null) {
        continue; // Something went wrong.
      }

      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }

      addTagsIfNotExists(editableFieldInfo.getGlobalTags(), tagUrns);
      changes.add(buildMetadataChangeProposal(resource.getUrn(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          editableSchemaMetadata));
    }

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveTagsToEntityProposals(
      List<Urn> tagUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) {
    final Map<Urn, GlobalTags> tagsAspects = getTagsAspects(
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
        new GlobalTags(),
        authentication
    );

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
      MetadataChangeProposal proposal = buildMetadataChangeProposal(
          resource.getUrn(),
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          globalTags
      );

      changes.add(proposal);
    }
    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveTagsToSubResourceProposals(
      List<Urn> tagUrns,
      List<ResourceReference> resources,
      @Nonnull Authentication authentication
  ) {
    final Map<Urn, EditableSchemaMetadata> editableSchemaMetadataAspects = getEditableSchemaMetadataAspects(
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
        new EditableSchemaMetadata(),
        authentication
    );

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {

      EditableSchemaMetadata editableSchemaMetadata = editableSchemaMetadataAspects.get(resource.getUrn());
      if (editableSchemaMetadata == null) {
        continue; // Something went wrong.
      }

      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, resource.getSubResource());

      if (!editableFieldInfo.hasGlossaryTerms()) {
        editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
      }
      removeTagsIfExists(editableFieldInfo.getGlobalTags(), tagUrns);
      changes.add(buildMetadataChangeProposal(resource.getUrn(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
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

  @Nonnull
  private Map<Urn, GlobalTags> getTagsAspects(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull GlobalTags defaultValue,
      @Nonnull Authentication authentication) {

    if (entityUrns.size() <= 0) {
      return Collections.emptyMap();
    }

    try {
      Map<Urn, Aspect> aspects = batchGetLatestAspect(
          entityUrns.stream().findFirst().get().getEntityType(), // TODO Improve this.
          entityUrns,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          this.entityClient,
          authentication
      );

      final Map<Urn, GlobalTags> finalResult = new HashMap<>();
      for (Urn entity : entityUrns) {
        Aspect aspect = aspects.get(entity);
        if (aspect == null) {
          finalResult.put(entity, defaultValue);
        } else {
          finalResult.put(entity, new GlobalTags(aspect.data()));
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving global tags for entities. Entities: {} aspect: {}",
          entityUrns,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          e);
      return Collections.emptyMap();
    }
  }

  @Nonnull
  private Map<Urn, EditableSchemaMetadata> getEditableSchemaMetadataAspects(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull EditableSchemaMetadata defaultValue,
      @Nonnull Authentication authentication) {

    if (entityUrns.size() <= 0) {
      return Collections.emptyMap();
    }

    try {
      Map<Urn, Aspect> aspects = batchGetLatestAspect(
          entityUrns.stream().findFirst().get().getEntityType(), // TODO Improve this.
          entityUrns,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          this.entityClient,
          authentication
      );

      final Map<Urn, EditableSchemaMetadata> finalResult = new HashMap<>();
      for (Urn entity : entityUrns) {
        Aspect aspect = aspects.get(entity);
        if (aspect == null) {
          finalResult.put(entity, defaultValue);
        } else {
          finalResult.put(entity, new EditableSchemaMetadata(aspect.data()));
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving editable schema metadata for entities. Entities: {} aspect: {}",
          entityUrns,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          e);
      return Collections.emptyMap();
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
