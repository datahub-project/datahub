package com.linkedin.metadata.service;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import  com.linkedin.entity.client.EntityClient;
import com.datahub.authentication.Authentication;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.entity.AspectUtils.*;


@Slf4j
public class GlossaryTermService extends BaseService {

  public GlossaryTermService(@Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }

  /**
   * Batch adds multiple glossary terms for a set of resources.
   *
   * @param glossaryTermUrns the urns of the terms to add
   * @param resources references to the resources to change
   *
   */
  public void batchAddGlossaryTerms(
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources) {
    batchAddGlossaryTerms(glossaryTermUrns, resources, this.systemAuthentication);
  }

  /**
   * Batch adds multiple glossary terms for a set of resources.
   *
   * @param glossaryTermUrns the urns of the terms to add
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   *
   */
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

  /**
   * Batch removes multiple glossary terms for a set of resources.
   *
   * @param glossaryTermUrns the urns of the terms to remove
   * @param resources references to the resources to change
   *
   */
  public void batchRemoveGlossaryTerms(
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources) {
    batchRemoveGlossaryTerms(glossaryTermUrns, resources, this.systemAuthentication);
  }

  /**
   * Batch removes multiple glossary terms for a set of resources.
   *
   * @param glossaryTermUrns the urns of the terms to remove
   * @param resources references to the resources to change
   * @param authentication authentication to use when making the change
   *
   */
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
      List<Urn> glossaryTerms,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws Exception {
    List<MetadataChangeProposal> changes = buildAddGlossaryTermsProposals(glossaryTerms, resources, authentication);
    ingestChangeProposals(changes, authentication);
  }

  private void removeGlossaryTermsFromResources(
      List<Urn> glossaryTerms,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws Exception {
    List<MetadataChangeProposal> changes = buildRemoveGlossaryTermsProposals(glossaryTerms, resources, authentication);
    ingestChangeProposals(changes, authentication);
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddGlossaryTermsProposals(
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws URISyntaxException {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs = resources.stream()
        .filter(resource -> resource.getSubResource() == null || resource.getSubResource().equals(""))
        .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals = buildAddGlossaryTermsToEntityProposals(glossaryTermUrns, entityRefs, authentication);

    final List<ResourceReference> schemaFieldRefs = resources.stream()
        .filter(resource -> resource.getSubResourceType() != null && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
        .collect(Collectors.toList());
    final List<MetadataChangeProposal> schemaFieldProposals = buildAddGlossaryTermsToSubResourceProposals(glossaryTermUrns, schemaFieldRefs, authentication);

    changes.addAll(entityProposals);
    changes.addAll(schemaFieldProposals);

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveGlossaryTermsProposals(
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs = resources.stream()
        .filter(resource -> resource.getSubResource() == null || resource.getSubResource().equals(""))
        .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals = buildRemoveGlossaryTermsToEntityProposals(glossaryTermUrns, entityRefs, authentication);

    final List<ResourceReference> schemaFieldRefs = resources.stream()
        .filter(resource -> resource.getSubResourceType() != null && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
        .collect(Collectors.toList());
    final List<MetadataChangeProposal> schemaFieldProposals = buildRemoveGlossaryTermsToSubResourceProposals(glossaryTermUrns, schemaFieldRefs, authentication);

    changes.addAll(entityProposals);
    changes.addAll(schemaFieldProposals);

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddGlossaryTermsToEntityProposals(
      List<com.linkedin.common.urn.Urn> glossaryTermUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) throws URISyntaxException {

    final Map<Urn, GlossaryTerms> glossaryTermAspects = getGlossaryTermsAspects(
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
        new GlossaryTerms(),
        authentication
    );

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {

      com.linkedin.common.GlossaryTerms glossaryTerms = glossaryTermAspects.get(resource.getUrn());
      if (glossaryTerms == null) {
        continue; // Something went wrong.
      }

      if (!glossaryTerms.hasTerms()) {
        glossaryTerms.setTerms(new GlossaryTermAssociationArray());
        glossaryTerms.setAuditStamp(new AuditStamp().setTime(System.currentTimeMillis()).setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr())));
      }
      addGlossaryTermsIfNotExists(glossaryTerms, glossaryTermUrns);
      changes.add(buildMetadataChangeProposal(resource.getUrn(), Constants.GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms));
    }
    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddGlossaryTermsToSubResourceProposals(
      final List<Urn> glossaryTermUrns,
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

      addGlossaryTermsIfNotExists(editableFieldInfo.getGlossaryTerms(), glossaryTermUrns);
      changes.add(buildMetadataChangeProposal(resource.getUrn(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          editableSchemaMetadata));
    }

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveGlossaryTermsToEntityProposals(
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources,
      Authentication authentication
  ) {

    final Map<Urn, GlossaryTerms> glossaryTermAspects = getGlossaryTermsAspects(
        resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
        new GlossaryTerms(),
        authentication
    );

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      com.linkedin.common.GlossaryTerms glossaryTerms = glossaryTermAspects.get(resource.getUrn());
      if (glossaryTerms == null) {
        continue; // Something went wrong.
      }
      if (!glossaryTerms.hasTerms()) {
        glossaryTerms.setTerms(new GlossaryTermAssociationArray());
        glossaryTerms.setAuditStamp(new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr())));
      }
      removeGlossaryTermsIfExists(glossaryTerms, glossaryTermUrns);
      MetadataChangeProposal proposal = buildMetadataChangeProposal(
          resource.getUrn(),
          Constants.GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms
      );

      changes.add(proposal);
    }
    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveGlossaryTermsToSubResourceProposals(
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources,
      Authentication authentication
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
      removeGlossaryTermsIfExists(editableFieldInfo.getGlossaryTerms(), glossaryTermUrns);
       changes.add(buildMetadataChangeProposal(resource.getUrn(), Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          editableSchemaMetadata));
    }

    return changes;
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
}
