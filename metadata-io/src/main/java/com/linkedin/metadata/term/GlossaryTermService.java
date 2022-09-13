package com.linkedin.metadata.term;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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

import static com.linkedin.metadata.entity.AspectUtils.*;


@Slf4j
public class GlossaryTermService {

  private final EntityClient entityClient;
  private final Authentication systemAuthentication;

  public GlossaryTermService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    this.entityClient = Objects.requireNonNull(entityClient);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
  }

  public void batchAddGlossaryTerms(
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources) {
    batchAddGlossaryTerms(glossaryTermUrns, resources, this.systemAuthentication);
  }

  public void batchAddGlossaryTerms(
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources,
      @Nonnull Authentication authentication) {
    log.debug("Batch adding GlossaryTerms to entities. glossaryTerms: {}, resources: {}", resources, glossaryTermUrns);
    try {
      addGlossaryTermsToResources(glossaryTermUrns, resources, authentication);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add GlossaryTerms %s to resources with urns %s!",
          glossaryTermUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  public void batchRemoveGlossaryTerms(
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources) {
    batchRemoveGlossaryTerms(glossaryTermUrns, resources, this.systemAuthentication);
  }

  public void batchRemoveGlossaryTerms(
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources,
      @Nonnull Authentication authentication) {
    log.debug("Batch adding GlossaryTerms to entities. glossaryTerms: {}, resources: {}", resources, glossaryTermUrns);
    try {
      removeGlossaryTermsFromResources(glossaryTermUrns, resources, authentication);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to batch add GlossaryTerms %s to resources with urns %s!",
          glossaryTermUrns,
          resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  private void addGlossaryTermsToResources(
      List<com.linkedin.common.urn.Urn> glossaryTermUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference entity : resources) {
      MetadataChangeProposal proposal = buildAddGlossaryTermsProposal(glossaryTermUrns, entity, authentication);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  private void removeGlossaryTermsFromResources(
      List<Urn> glossaryTerms,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws Exception {
    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      MetadataChangeProposal proposal = buildRemoveGlossaryTermsProposal(glossaryTerms, resource, authentication);
      if (proposal != null) {
        changes.add(proposal);
      }
    }
    ingestChangeProposals(changes, authentication);
  }

  @Nullable
  private MetadataChangeProposal buildAddGlossaryTermsProposal(
      List<com.linkedin.common.urn.Urn> glossaryTermUrns,
      ResourceReference resource,
      Authentication authentication
  ) throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding glossaryTerms to a top-level entity
      return buildAddGlossaryTermsToEntityProposal(glossaryTermUrns, resource, authentication);
    } else {
      // Case 2: Adding glossaryTerms to subresource (e.g. schema fields)
      return buildAddGlossaryTermsToSubResourceProposal(glossaryTermUrns, resource, authentication);
    }
  }

  @Nullable
  private MetadataChangeProposal buildRemoveGlossaryTermsProposal(
      List<Urn> glossaryTermUrns,
      ResourceReference resource,
      Authentication authentication
  ) throws URISyntaxException {
    if (resource.getSubResource() == null || resource.getSubResource().equals("")) {
      // Case 1: Adding glossaryTerms to a top-level entity
      return buildRemoveGlossaryTermsToEntityProposal(glossaryTermUrns, resource, authentication);
    } else {
      // Case 2: Adding glossaryTerms to subresource (e.g. schema fields)
      return buildRemoveGlossaryTermsToSubResourceProposal(glossaryTermUrns, resource, authentication);
    }
  }

  @Nullable
  private MetadataChangeProposal buildAddGlossaryTermsToEntityProposal(
      List<com.linkedin.common.urn.Urn> glossaryTermUrns,
      ResourceReference resource,
      Authentication authentication
  ) throws URISyntaxException {
    com.linkedin.common.GlossaryTerms glossaryTerms =
        getGlossaryTermsAspect(
            resource.getUrn(),
            new GlossaryTerms(),
            authentication);

    if (glossaryTerms == null) {
      return null;
    }

    if (!glossaryTerms.hasTerms()) {
      glossaryTerms.setTerms(new GlossaryTermAssociationArray());
      glossaryTerms.setAuditStamp(new AuditStamp()
          .setTime(System.currentTimeMillis())
          .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr())));
    }
    addGlossaryTermsIfNotExists(glossaryTerms, glossaryTermUrns);
    return buildMetadataChangeProposal(resource.getUrn(), Constants.GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms);
  }

  @Nullable
  private MetadataChangeProposal buildRemoveGlossaryTermsToEntityProposal(
      List<Urn> glossaryTermUrns,
      ResourceReference resource,
      Authentication authentication
  ) {
    com.linkedin.common.GlossaryTerms glossaryTerms = getGlossaryTermsAspect(
        resource.getUrn(),
        new GlossaryTerms(),
        authentication);

    if (glossaryTerms == null) {
      return null;
    }

    if (!glossaryTerms.hasTerms()) {
      glossaryTerms.setTerms(new GlossaryTermAssociationArray());
      glossaryTerms.setAuditStamp(new AuditStamp()
          .setTime(System.currentTimeMillis())
          .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr())));
    }
    removeGlossaryTermsIfExists(glossaryTerms, glossaryTermUrns);
    return buildMetadataChangeProposal(
        resource.getUrn(),
        Constants.GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms
    );
  }

  @Nullable
  private MetadataChangeProposal buildRemoveGlossaryTermsToSubResourceProposal(
      List<Urn> glossaryTermUrns,
      ResourceReference resource,
      Authentication authentication
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

    if (!editableFieldInfo.hasGlossaryTerms()) {
      editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
    }
    removeGlossaryTermsIfExists(editableFieldInfo.getGlossaryTerms(), glossaryTermUrns);
    return buildMetadataChangeProposal(resource.getUrn(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME, editableSchemaMetadata);
  }

  @Nullable
  private MetadataChangeProposal buildAddGlossaryTermsToSubResourceProposal(
      final List<Urn> glossaryTermUrns,
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

    if (!editableFieldInfo.hasGlossaryTerms()) {
      editableFieldInfo.setGlossaryTerms(new GlossaryTerms());
    }

    addGlossaryTermsIfNotExists(editableFieldInfo.getGlossaryTerms(), glossaryTermUrns);
    return buildMetadataChangeProposal(
        resource.getUrn(),
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        editableSchemaMetadata);
  }

  private void addGlossaryTermsIfNotExists(GlossaryTerms glossaryTerms, List<Urn> glossaryTermUrns) throws URISyntaxException {
    if (!glossaryTerms.hasTerms()) {
      glossaryTerms.setTerms(new GlossaryTermAssociationArray());
    }

    GlossaryTermAssociationArray glossaryTermAssociationArray = glossaryTerms.getTerms();

    List<Urn> glossaryTermsToAdd = new ArrayList<>();
    for (Urn glossaryTermUrn : glossaryTermUrns) {
      if (glossaryTermAssociationArray.stream().anyMatch(association -> association.getUrn().equals(glossaryTermUrn))) {
        continue;
      }
      glossaryTermsToAdd.add(glossaryTermUrn);
    }

    // Check for no glossaryTerms to add
    if (glossaryTermsToAdd.size() == 0) {
      return;
    }

    for (Urn glossaryTermUrn : glossaryTermsToAdd) {
      GlossaryTermAssociation newAssociation = new GlossaryTermAssociation();
      newAssociation.setUrn(GlossaryTermUrn.createFromUrn(glossaryTermUrn));
      glossaryTermAssociationArray.add(newAssociation);
    }
  }

  private static GlossaryTermAssociationArray removeGlossaryTermsIfExists(GlossaryTerms glossaryTerms, List<Urn> glossaryTermUrns) {
    if (!glossaryTerms.hasTerms()) {
      glossaryTerms.setTerms(new GlossaryTermAssociationArray());
    }
    GlossaryTermAssociationArray glossaryTermAssociationArray = glossaryTerms.getTerms();
    for (Urn glossaryTermUrn : glossaryTermUrns) {
      glossaryTermAssociationArray.removeIf(association -> association.getUrn().equals(glossaryTermUrn));
    }
    return glossaryTermAssociationArray;
  }

  @Nullable
  private GlossaryTerms getGlossaryTermsAspect(
      @Nonnull Urn entityUrn,
      @Nonnull GlossaryTerms defaultValue,
      @Nonnull Authentication authentication) {
    try {
      Aspect aspect = getLatestAspect(
          entityUrn,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          this.entityClient,
          authentication
      );

      if (aspect == null) {
        return defaultValue;
      }
      return new GlossaryTerms(aspect.data());
    } catch (Exception e) {
      log.error(
          "Error retrieving glossaryTerms for entity. Entity: {} aspect: {}",
          entityUrn,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
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

  private void ingestChangeProposals(@Nonnull List<MetadataChangeProposal> changes, @Nonnull Authentication authentication) throws Exception {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      this.entityClient.ingestProposal(change, authentication);
    }
  }
}
