package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.METADATA_TESTS_SOURCE;
import static com.linkedin.metadata.entity.AspectUtils.*;
import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.GlossaryTermsPatchBuilder;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GlossaryTermService extends BaseService {

  private final boolean _isAsync;

  public GlossaryTermService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      final boolean isAsync) {
    super(entityClient, openApiClient, objectMapper);
    _isAsync = isAsync;
  }

  public GlossaryTermService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
    _isAsync = false;
  }

  /**
   * Batch adds multiple glossary terms for a set of resources.
   *
   * @param glossaryTermUrns the urns of the terms to add
   * @param resources references to the resources to change
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public void batchAddGlossaryTerms(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource,
      @Nullable Urn actorUrn) {
    log.debug(
        "Batch adding GlossaryTerms to entities. glossaryTerms: {}, resources: {}",
        resources,
        glossaryTermUrns);
    try {
      addGlossaryTermsToResources(opContext, glossaryTermUrns, resources, appSource, actorUrn);
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
   * @param appSource optional indication of the origin for this request, used for additional
   *     processing logic when matching particular sources
   */
  public void batchRemoveGlossaryTerms(
      @Nonnull OperationContext opContext,
      @Nonnull List<Urn> glossaryTermUrns,
      @Nonnull List<ResourceReference> resources,
      @Nullable String appSource) {
    log.debug(
        "Batch adding GlossaryTerms to entities. glossaryTerms: {}, resources: {}",
        resources,
        glossaryTermUrns);
    try {
      removeGlossaryTermsFromResources(opContext, glossaryTermUrns, resources, appSource);
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
   * Retrieve all asset level terms for a given asset.
   *
   * @param opContext the operation context
   * @param entityUrn the entity to retrieve terms for.
   * @return the term associations associated with the entity.
   */
  public List<GlossaryTermAssociation> getEntityTerms(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final GlossaryTerms maybeTerms = getGlossaryTerms(opContext, entityUrn);
    if (maybeTerms != null) {
      return maybeTerms.getTerms();
    }
    return Collections.emptyList();
  }

  /**
   * Retrieve all schema-field level terms for a given asset.
   *
   * @param opContext the operation context
   * @param entityUrn the entity to retrieve terms for
   * @return the term associations associated with the schema field
   */
  public List<GlossaryTermAssociation> getSchemaFieldTerms(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final List<GlossaryTermAssociation> editableSchemaFieldTerms =
        getEditableSchemaFieldGlossaryTerms(opContext, entityUrn, fieldPath);
    final List<GlossaryTermAssociation> schemaFieldTerms =
        getNonEditableSchemaFieldGlossaryTerms(opContext, entityUrn, fieldPath);

    List<GlossaryTermAssociation> result = new ArrayList<>();
    result.addAll(editableSchemaFieldTerms);
    result.addAll(schemaFieldTerms);

    return result;
  }

  private List<GlossaryTermAssociation> getEditableSchemaFieldGlossaryTerms(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final EditableSchemaFieldInfo maybeEditableSchemaField =
        getEditableSchemaField(opContext, entityUrn, fieldPath);
    if (maybeEditableSchemaField != null && maybeEditableSchemaField.hasGlossaryTerms()) {
      return maybeEditableSchemaField.getGlossaryTerms().getTerms();
    }
    return Collections.emptyList();
  }

  private List<GlossaryTermAssociation> getNonEditableSchemaFieldGlossaryTerms(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final SchemaField maybeSchemaField = getSchemaField(opContext, entityUrn, fieldPath);
    if (maybeSchemaField != null && maybeSchemaField.hasGlossaryTerms()) {
      return maybeSchemaField.getGlossaryTerms().getTerms();
    }
    return Collections.emptyList();
  }

  @Nullable
  private GlossaryTerms getGlossaryTerms(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getGlossaryTermsEntityResponse(opContext, entityUrn);
    if (response != null && response.getAspects().containsKey(GLOSSARY_TERMS_ASPECT_NAME)) {
      return new GlossaryTerms(
          response.getAspects().get(GLOSSARY_TERMS_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  private EntityResponse getGlossaryTermsEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(GLOSSARY_TERMS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve glossary terms for entity with urn %s", entityUrn), e);
    }
  }

  private void addGlossaryTermsToResources(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTerms,
      List<ResourceReference> resources,
      @Nullable String appSource,
      @Nullable Urn actorUrn)
      throws Exception {
    List<MetadataChangeProposal> changes =
        buildAddGlossaryTermsProposals(opContext, glossaryTerms, resources, appSource, actorUrn);
    if (appSource != null) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  private void removeGlossaryTermsFromResources(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTerms,
      List<ResourceReference> resources,
      @Nullable String appSource)
      throws Exception {
    List<MetadataChangeProposal> changes =
        buildRemoveGlossaryTermsProposals(opContext, glossaryTerms, resources);
    if (appSource != null) {
      applyAppSource(changes, appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  @VisibleForTesting
  List<MetadataChangeProposal> buildAddGlossaryTermsProposals(
      @Nonnull OperationContext opContext,
      List<Urn> glossaryTermUrns,
      List<ResourceReference> resources,
      @Nullable String appSource,
      @Nullable Urn actorUrn)
      throws URISyntaxException {

    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final List<ResourceReference> entityRefs =
        resources.stream()
            .filter(
                resource ->
                    resource.getSubResource() == null || resource.getSubResource().equals(""))
            .collect(Collectors.toList());
    final List<MetadataChangeProposal> entityProposals =
        buildAddGlossaryTermsToEntityProposals(
            opContext, glossaryTermUrns, entityRefs, appSource, actorUrn);

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
      List<ResourceReference> resources,
      @Nullable String appSource,
      @Nullable Urn actorUrn)
      throws URISyntaxException {
    final List<MetadataChangeProposal> changes = new ArrayList<>();

    final Urn finalActorUrn =
        actorUrn != null
            ? actorUrn
            : UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr());
    AuditStamp lastModified =
        new AuditStamp().setTime(System.currentTimeMillis()).setActor(finalActorUrn);

    if (appSource != null && appSource.equals(METADATA_TESTS_SOURCE)) {
      return patchAddGlossaryTerms(glossaryTermUrns, resources, lastModified);
    }

    final Map<Urn, GlossaryTerms> glossaryTermAspects =
        getGlossaryTermsAspects(
            opContext,
            resources.stream().map(ResourceReference::getUrn).collect(Collectors.toSet()),
            new GlossaryTerms());

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
      addGlossaryTermsIfNotExists(opContext, glossaryTerms, glossaryTermUrns);
      changes.add(
          buildMetadataChangeProposal(
              resource.getUrn(), Constants.GLOSSARY_TERMS_ASPECT_NAME, glossaryTerms));
    }
    return changes;
  }

  List<MetadataChangeProposal> patchAddGlossaryTerms(
      List<com.linkedin.common.urn.Urn> ownerUrns,
      List<ResourceReference> resources,
      AuditStamp lastModified) {
    final List<MetadataChangeProposal> mcps = new ArrayList<>();
    for (ResourceReference resource : resources) {
      GlossaryTermsPatchBuilder patchBuilder =
          new GlossaryTermsPatchBuilder().urn(resource.getUrn());
      for (Urn ownerUrn : ownerUrns) {
        patchBuilder.addTerm(ownerUrn, null);
      }
      patchBuilder.addAuditStamp(lastModified);
      mcps.add(patchBuilder.build());
    }
    return mcps;
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

      addGlossaryTermsIfNotExists(
          opContext, editableFieldInfo.getGlossaryTerms(), glossaryTermUrns);
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

  private void addGlossaryTermsIfNotExists(
      @Nonnull OperationContext opContext, GlossaryTerms glossaryTerms, List<Urn> glossaryTermUrns)
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

    glossaryTerms.setAuditStamp(opContext.getAuditStamp());
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
