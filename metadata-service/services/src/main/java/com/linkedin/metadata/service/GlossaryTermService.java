package com.linkedin.metadata.service;

import static com.linkedin.metadata.entity.AspectUtils.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
public class GlossaryTermService extends BaseService {

  public GlossaryTermService(@Nonnull SystemEntityClient entityClient) {
    super(entityClient);
  }

  /**
   * Batch adds multiple glossary terms for a set of resources.
   *
   * @param glossaryTermUrns the urns of the terms to add
   * @param resources references to the resources to change
   */
  public void batchAddGlossaryTerms(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources) {
    log.debug(
        "Batch adding GlossaryTerms to entities. glossaryTerms: {}, resources: {}",
        resources,
        glossaryTermUrns);
    try {
      addGlossaryTermsToResources(opContext, glossaryTermUrns, resources);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add GlossaryTerms %s to resources with urns %s!",
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
   */
  public void batchRemoveGlossaryTerms(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources) {
    log.debug(
        "Batch adding GlossaryTerms to entities. glossaryTerms: {}, resources: {}",
        resources,
        glossaryTermUrns);
    try {
      removeGlossaryTermsFromResources(opContext, glossaryTermUrns, resources);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch add GlossaryTerms %s to resources with urns %s!",
              glossaryTermUrns,
              resources.stream().map(ResourceReference::getUrn).collect(Collectors.toList())),
          e);
    }
  }

  private void addGlossaryTermsToResources(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTerms,
      List<ResourceReference> resources)
      throws Exception {
    List<MetadataChangeProposal> changes =
        buildAddGlossaryTermsProposals(opContext, glossaryTerms, resources);
    ingestChangeProposals(opContext, changes);
  }

  private void removeGlossaryTermsFromResources(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTerms,
      List<ResourceReference> resources)
      throws Exception {
    List<MetadataChangeProposal> changes =
        buildRemoveGlossaryTermsProposals(opContext, glossaryTerms, resources);
    ingestChangeProposals(opContext, changes);
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddGlossaryTermsProposals(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources)
      throws URISyntaxException {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResource() == null || resource.getSubResource().equals(""))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals =
        buildAddGlossaryTermsToEntityProposals(opContext, glossaryTermUrns, entityRefs);

    final List<ResourceReference> schemaFieldRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResourceType() != null
                        && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> schemaFieldProposals =
        buildAddGlossaryTermsToSubResourceProposals(opContext, glossaryTermUrns, schemaFieldRefs);

    changes.addAll(entityProposals);
    changes.addAll(schemaFieldProposals);

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveGlossaryTermsProposals(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources) {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResource() == null || resource.getSubResource().equals(""))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals =
        buildRemoveGlossaryTermsToEntityProposals(opContext, glossaryTermUrns, entityRefs);

    final List<ResourceReference> schemaFieldRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResourceType() != null
                        && resource.getSubResourceType().equals(SubResourceType.DATASET_FIELD))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> schemaFieldProposals =
        buildRemoveGlossaryTermsToSubResourceProposals(
            opContext, glossaryTermUrns, schemaFieldRefs);

    changes.addAll(entityProposals);
    changes.addAll(schemaFieldProposals);

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddGlossaryTermsToEntityProposals(
      @Nonnull OperationContext opContext,
      List<com.linkedin.common.urn.Urn> glossaryTermUrns,
      List<ResourceReference> resources)
      throws URISyntaxException {

    final Map<Urn, GlossaryTerms> glossaryTermAspects =
        getGlossaryTermsAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new GlossaryTerms());

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {

      com.linkedin.common.GlossaryTerms glossaryTerms = glossaryTermAspects.get(resource.getUrn());
      if (glossaryTerms == null) {
        continue; // Something went wrong.
      }

      if (!glossaryTerms.hasTerms()) {
        glossaryTerms.setTerms(new GlossaryTermAssociationArray());
        glossaryTerms.setAuditStamp(
            new AuditStamp()
                .setTime(System.currentTimeMillis())
                .setActor(
                    UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr())));
      }
      addGlossaryTermsIfNotExists(glossaryTerms, glossaryTermUrns);
      changes.add(
          buildMetadataChangeProposal(
              resource.getUrn(), Constants.GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms));
    }
    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddGlossaryTermsToSubResourceProposals(
      @Nonnull OperationContext opContext,
      final List<Urn> glossaryTermUrns,
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

      addGlossaryTermsIfNotExists(editableFieldInfo.getGlossaryTerms(), glossaryTermUrns);
      changes.add(
          buildMetadataChangeProposal(
              resource.getUrn(),
              Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
              editableSchemaMetadata));
    }

    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveGlossaryTermsToEntityProposals(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources) {

    final Map<Urn, GlossaryTerms> glossaryTermAspects =
        getGlossaryTermsAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new GlossaryTerms());

    final List<MetadataChangeProposal> changes = new ArrayList<>();
    for (ResourceReference resource : resources) {
      com.linkedin.common.GlossaryTerms glossaryTerms = glossaryTermAspects.get(resource.getUrn());
      if (glossaryTerms == null) {
        continue; // Something went wrong.
      }
      if (!glossaryTerms.hasTerms()) {
        glossaryTerms.setTerms(new GlossaryTermAssociationArray());
        glossaryTerms.setAuditStamp(
            new AuditStamp()
                .setTime(System.currentTimeMillis())
                .setActor(
                    UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr())));
      }
      removeGlossaryTermsIfExists(glossaryTerms, glossaryTermUrns);
      MetadataChangeProposal proposal =
          buildMetadataChangeProposal(
              resource.getUrn(), Constants.GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms);

      changes.add(proposal);
    }
    return changes;
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildRemoveGlossaryTermsToSubResourceProposals(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources) {

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
      removeGlossaryTermsIfExists(editableFieldInfo.getGlossaryTerms(), glossaryTermUrns);
      changes.add(
          buildMetadataChangeProposal(
              resource.getUrn(),
              Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
              editableSchemaMetadata));
    }

    return changes;
  }

  private void addGlossaryTermsIfNotExists(GlossaryTerms glossaryTerms, List<Urn> glossaryTermUrns)
      throws URISyntaxException {
    if (!glossaryTerms.hasTerms()) {
      glossaryTerms.setTerms(new GlossaryTermAssociationArray());
    }

    GlossaryTermAssociationArray glossaryTermAssociationArray = glossaryTerms.getTerms();

    List<Urn> glossaryTermsToAdd = new ArrayList<>();
    for (Urn glossaryTermUrn : glossaryTermUrns) {
      if (glossaryTermAssociationArray.stream()
          .anyMatch(association -> association.getUrn().equals(glossaryTermUrn))) {
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

  private static GlossaryTermAssociationArray removeGlossaryTermsIfExists(
      GlossaryTerms glossaryTerms, List<Urn> glossaryTermUrns) {
    if (!glossaryTerms.hasTerms()) {
      glossaryTerms.setTerms(new GlossaryTermAssociationArray());
    }
    GlossaryTermAssociationArray glossaryTermAssociationArray = glossaryTerms.getTerms();
    for (Urn glossaryTermUrn : glossaryTermUrns) {
      glossaryTermAssociationArray.removeIf(
          association -> association.getUrn().equals(glossaryTermUrn));
    }
    return glossaryTermAssociationArray;
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
