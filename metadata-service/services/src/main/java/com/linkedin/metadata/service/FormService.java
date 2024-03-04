package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.FORMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.entity.AspectUtils.buildMetadataChangeProposal;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FieldFormPromptAssociation;
import com.linkedin.common.FieldFormPromptAssociationArray;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormPromptAssociationArray;
import com.linkedin.common.FormPromptFieldAssociations;
import com.linkedin.common.FormVerificationAssociation;
import com.linkedin.common.FormVerificationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.OwnershipUtils;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.service.util.SearchBasedFormAssignmentRunner;
import com.linkedin.metadata.utils.FormUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to execute CRUD operations around forms and submitting responses to forms and
 * their prompts.
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class FormService extends BaseService {
  private static final int BATCH_FORM_ENTITY_COUNT = 500;

  private final OperationContext systemOpContext;

  public FormService(
      @Nonnull OperationContext systemOpContext, @Nonnull final EntityClient entityClient) {
    super(entityClient, systemOpContext.getAuthentication());
    this.systemOpContext = systemOpContext;
  }

  /** Batch associated a form to a given set of entities by urn. */
  public void batchAssignFormToEntities(
      @Nonnull final List<Urn> entityUrns, @Nonnull final Urn formUrn) throws Exception {
    batchAssignFormToEntities(entityUrns, formUrn, this.systemAuthentication);
  }

  /** Batch associated a form to a given set of entities by urn. */
  public void batchAssignFormToEntities(
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final Authentication authentication)
      throws Exception {
    verifyEntityExists(formUrn, authentication);
    verifyEntitiesExist(entityUrns, authentication);
    final List<MetadataChangeProposal> changes =
        buildAssignFormChanges(entityUrns, formUrn, authentication);
    ingestChangeProposals(changes, authentication);
  }

  /** Batch remove a form from a given entity by urn. */
  public void batchUnassignFormForEntities(
      @Nonnull final List<Urn> entityUrns, @Nonnull final Urn formUrn) throws Exception {
    batchUnassignFormForEntities(entityUrns, formUrn, this.systemAuthentication);
  }

  /** Batch remove a form from a given entity by urn. */
  public void batchUnassignFormForEntities(
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final Authentication authentication)
      throws Exception {
    verifyEntityExists(formUrn, authentication);
    verifyEntitiesExist(entityUrns, authentication);
    final List<MetadataChangeProposal> changes =
        buildUnassignFormChanges(entityUrns, formUrn, authentication);
    ingestChangeProposals(changes, authentication);
  }

  /** Mark a specific form prompt as incomplete */
  public void batchSetFormPromptIncomplete(
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId)
      throws Exception {
    batchSetFormPromptIncomplete(entityUrns, formUrn, formPromptId, this.systemAuthentication);
  }

  /** Mark a specific form prompt as incomplete */
  public void batchSetFormPromptIncomplete(
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final Authentication authentication)
      throws Exception {
    verifyEntityExists(formUrn, authentication);
    verifyEntitiesExist(entityUrns, authentication);
    final FormInfo formInfo = getFormInfo(formUrn, authentication);
    final List<MetadataChangeProposal> changes =
        buildUnsetFormPromptChanges(entityUrns, formUrn, formPromptId, formInfo, authentication);
    ingestChangeProposals(changes, authentication);
  }

  /** Create a dynamic form assignment for a particular form. */
  public void createDynamicFormAssignment(
      @Nonnull final DynamicFormAssignment dynamicFormAssignment,
      @Nonnull final Urn formUrn,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    if (!entityClient.exists(formUrn, authentication)) {
      throw new RuntimeException(
          String.format("Form %s does not exist. Skipping dynamic form assignment", formUrn));
    }

    try {
      this.entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(
              formUrn, Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME, dynamicFormAssignment),
          authentication,
          false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create form", e);
    }
  }

  /** Assigns the form to an entity for completion. */
  public void upsertFormAssignmentRunner(
      @Nonnull final Urn formUrn, @Nonnull final DynamicFormAssignment formFilters) {
    try {
      SearchBasedFormAssignmentRunner.assign(
          systemOpContext, formFilters, formUrn, BATCH_FORM_ENTITY_COUNT, entityClient);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to dynamically assign form with urn: %s", formUrn), e);
    }
  }

  /** Submit a response for a structured property type prompt. */
  public Boolean batchSubmitStructuredPropertyPromptResponse(
      @Nonnull final List<String> entityUrns,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final Authentication authentication)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitStructuredPropertyPromptResponse(
                urn, structuredPropertyUrn, values, formUrn, formPromptId, authentication);
          } catch (Exception e) {
            throw new RuntimeException("Failed to batch submit structured property prompt", e);
          }
        });

    return true;
  }

  /** Submit a response for a structured property type prompt. */
  public Boolean submitStructuredPropertyPromptResponse(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final Authentication authentication)
      throws Exception {

    // First, let's apply the action and add the structured property.
    ingestStructuredProperties(entityUrn, structuredPropertyUrn, values, authentication);

    // Then, let's apply the change to the entity's form status.
    ingestCompletedFormResponse(entityUrn, formUrn, formPromptId, authentication);

    return true;
  }

  /** Submit a response for a field-level structured property type prompt. */
  public Boolean batchSubmitFieldStructuredPropertyPromptResponse(
      @Nonnull final List<String> entityUrns,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final String fieldPath,
      @Nonnull final Authentication authentication)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitFieldStructuredPropertyPromptResponse(
                urn,
                structuredPropertyUrn,
                values,
                formUrn,
                formPromptId,
                fieldPath,
                authentication);
          } catch (Exception e) {
            throw new RuntimeException(
                "Failed to batch submit field structured property prompt", e);
          }
        });

    return true;
  }

  /** Submit a response for a field-level structured property type prompt. */
  public Boolean submitFieldStructuredPropertyPromptResponse(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final String fieldPath,
      @Nonnull final Authentication authentication)
      throws Exception {

    // First, let's apply the action and add the structured property.
    ingestSchemaFieldStructuredProperties(
        entityUrn, structuredPropertyUrn, values, fieldPath, authentication);

    // Then, let's apply the change to the entity's form status.
    ingestCompletedFieldFormResponse(entityUrn, formUrn, formPromptId, fieldPath, authentication);

    return true;
  }

  private void ingestCompletedFieldFormResponse(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final String fieldPath,
      @Nonnull final Authentication authentication)
      throws Exception {
    final Forms forms = getEntityForms(entityUrn, authentication);
    final FormAssociation formAssociation = getFormWithUrn(forms, formUrn);
    if (formAssociation == null) {
      throw new RuntimeException(
          String.format("Form %s has not been assigned to entity %s", formUrn, entityUrn));
    }
    final FormPromptAssociation formPromptAssociation =
        getOrDefaultFormPromptAssociation(formAssociation, formPromptId, authentication);

    // update the prompt association to have this fieldFormPromptAssociation marked as complete
    updateFieldPromptToComplete(
        formPromptAssociation, fieldPath, UrnUtils.getUrn(authentication.getActor().toUrnStr()));

    // field prompt is complete if all fields in entity's schema metadata are marked complete
    if (isFieldPromptComplete(entityUrn, formPromptAssociation, authentication)) {
      // if this is complete, the prompt as a whole should be marked as complete
      ingestCompletedFormResponse(entityUrn, formUrn, formPromptId, forms, authentication);
    } else {
      // regardless, ingest forms to save state of this aspect
      ingestForms(entityUrn, forms, authentication);
    }
  }

  private void ingestCompletedFormResponse(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final Authentication authentication)
      throws Exception {
    final Forms forms = getEntityForms(entityUrn, authentication);
    ingestCompletedFormResponse(entityUrn, formUrn, formPromptId, forms, authentication);
  }

  private void ingestCompletedFormResponse(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final Forms forms,
      @Nonnull final Authentication authentication)
      throws Exception {
    // Next, get all the information we need to update the forms for the entity.
    final FormInfo formInfo = getFormInfo(formUrn, authentication);
    final FormAssociation formAssociation = getFormWithUrn(forms, formUrn);

    if (formAssociation == null) {
      throw new RuntimeException(
          String.format("Form %s has not been assigned to entity %s", formUrn, entityUrn));
    }

    // First, mark the prompt as completed in forms aspect.
    updatePromptToComplete(formAssociation, entityUrn, formUrn, formPromptId, authentication);

    // Then, update the completed forms fields based on which prompts remain incomplete.
    updateFormCompletion(forms, formAssociation, formInfo);

    // Finally, ingest the newly updated forms aspect.
    ingestForms(entityUrn, forms, authentication);
  }

  private void ingestSchemaFieldStructuredProperties(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final String fieldPath,
      @Nonnull final Authentication authentication)
      throws Exception {
    Urn schemaFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(entityUrn.toString(), fieldPath);
    ingestStructuredProperties(schemaFieldUrn, structuredPropertyUrn, values, authentication);
  }

  private void ingestStructuredProperties(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Authentication authentication)
      throws Exception {
    final EntityResponse response =
        entityClient.getV2(
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME),
            authentication);

    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(new StructuredPropertyValueAssignmentArray());
    if (response != null && response.getAspects().containsKey(STRUCTURED_PROPERTIES_ASPECT_NAME)) {
      structuredProperties =
          new StructuredProperties(
              response.getAspects().get(STRUCTURED_PROPERTIES_ASPECT_NAME).getValue().data());
    }

    // Since we upsert assignments for this structuredProperty,
    // remove anything from this structured property and add to this list
    List<StructuredPropertyValueAssignment> filteredAssignments =
        structuredProperties.getProperties().stream()
            .filter(assignment -> !assignment.getPropertyUrn().equals(structuredPropertyUrn))
            .collect(Collectors.toList());

    StructuredPropertyValueAssignment assignment = new StructuredPropertyValueAssignment();
    assignment.setValues(values);
    assignment.setPropertyUrn(structuredPropertyUrn);
    assignment.setCreated(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr()))
            .setTime(System.currentTimeMillis()));
    assignment.setLastModified(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr()))
            .setTime(System.currentTimeMillis()));
    filteredAssignments.add(assignment);

    StructuredPropertyValueAssignmentArray assignments =
        new StructuredPropertyValueAssignmentArray(filteredAssignments);
    structuredProperties.setProperties(assignments);

    final MetadataChangeProposal structuredPropertiesProposal =
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);
    try {
      this.entityClient.ingestProposal(structuredPropertiesProposal, authentication, false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to submit form response", e);
    }
  }

  private void ingestForms(
      @Nonnull final Urn entityUrn,
      @Nonnull final Forms forms,
      @Nonnull final Authentication authentication) {
    try {
      ingestChangeProposals(
          ImmutableList.of(
              AspectUtils.buildMetadataChangeProposal(entityUrn, FORMS_ASPECT_NAME, forms)),
          authentication);
    } catch (Exception e) {
      log.warn(String.format("Failed to ingest forms for entity with urn %s", entityUrn), e);
    }
  }

  private Forms getEntityForms(
      @Nonnull final Urn entityUrn, @Nonnull final Authentication authentication) throws Exception {
    final EntityResponse response =
        entityClient.getV2(
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(FORMS_ASPECT_NAME),
            authentication);
    if (response != null && response.getAspects().containsKey(FORMS_ASPECT_NAME)) {
      return new Forms(response.getAspects().get(FORMS_ASPECT_NAME).getValue().data());
    }
    // No entity forms found.
    throw new RuntimeException(
        String.format(
            "Entity is missing forms aspect, form is not assigned to entity with urn %s",
            entityUrn));
  }

  /**
   * Checks schema metadata for an entity and ensures there's a completed field prompt for every
   * field. If there is no schema metadata, raise an error.
   */
  private boolean isFieldPromptComplete(
      @Nonnull final Urn entityUrn,
      @Nonnull final FormPromptAssociation formPromptAssociation,
      @Nonnull final Authentication authentication)
      throws Exception {
    final Set<String> completedFieldPaths =
        Objects.requireNonNull(formPromptAssociation.getFieldAssociations())
            .getCompletedFieldPrompts()
            .stream()
            .map(FieldFormPromptAssociation::getFieldPath)
            .collect(Collectors.toSet());
    final SchemaMetadata schemaMetadata = getSchemaMetadata(entityUrn, authentication);
    final List<String> fieldPaths =
        schemaMetadata.getFields().stream()
            .map(SchemaField::getFieldPath)
            .collect(Collectors.toList());

    return completedFieldPaths.containsAll(fieldPaths);
  }

  /**
   * Performs the operation of changing the status of a form field prompt from incomplete to
   * complete.
   */
  private void updateFieldPromptToComplete(
      @Nonnull final FormPromptAssociation formPromptAssociation,
      @Nonnull final String fieldPath,
      @Nonnull final Urn actor) {
    final FieldFormPromptAssociation completedFieldPromptAssociation =
        new FieldFormPromptAssociation();
    completedFieldPromptAssociation.setFieldPath(fieldPath);
    completedFieldPromptAssociation.setLastModified(createAuditStamp(actor));

    FormPromptFieldAssociations fieldAssociations =
        formPromptAssociation.getFieldAssociations() != null
            ? formPromptAssociation.getFieldAssociations()
            : new FormPromptFieldAssociations();

    if (fieldAssociations.getCompletedFieldPrompts() == null) {
      fieldAssociations.setCompletedFieldPrompts(new FieldFormPromptAssociationArray());
    }
    if (fieldAssociations.getIncompleteFieldPrompts() == null) {
      fieldAssociations.setIncompleteFieldPrompts(new FieldFormPromptAssociationArray());
    }

    // add this prompt association to list of completed prompts, removing its previous association
    // if it was already in there
    FieldFormPromptAssociationArray completedFieldPrompts =
        new FieldFormPromptAssociationArray(
            fieldAssociations.getCompletedFieldPrompts().stream()
                .filter(fieldPrompt -> !fieldPrompt.getFieldPath().equals(fieldPath))
                .collect(Collectors.toList()));
    completedFieldPrompts.add(completedFieldPromptAssociation);
    fieldAssociations.setCompletedFieldPrompts(completedFieldPrompts);

    // remove this prompt association from list of incomplete prompts
    FieldFormPromptAssociationArray incompleteFieldPrompts = new FieldFormPromptAssociationArray();
    fieldAssociations
        .getIncompleteFieldPrompts()
        .forEach(
            incompleteFieldPrompt -> {
              if (!incompleteFieldPrompt.getFieldPath().equals(fieldPath)) {
                incompleteFieldPrompts.add(incompleteFieldPrompt);
              }
            });
    fieldAssociations.setIncompleteFieldPrompts(incompleteFieldPrompts);

    formPromptAssociation.setFieldAssociations(fieldAssociations);
  }

  /** Performs the operation of changing the status of a form prompt from incomplete to complete. */
  private void updatePromptToComplete(
      @Nonnull final FormAssociation formAssociation,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final Authentication authentication) {
    final FormPromptAssociation formPromptAssociation =
        getOrDefaultFormPromptAssociation(formAssociation, formPromptId, authentication);

    // add this prompt association to list of completed prompts, removing its previous association
    // if it was already in there
    FormPromptAssociationArray completedPrompts =
        new FormPromptAssociationArray(
            formAssociation.getCompletedPrompts().stream()
                .filter(prompt -> !prompt.getId().equals(formPromptId))
                .collect(Collectors.toList()));
    completedPrompts.add(formPromptAssociation);
    formAssociation.setCompletedPrompts(completedPrompts);

    // remove this prompt association from list of incomplete prompts
    FormPromptAssociationArray incompletePrompts = new FormPromptAssociationArray();
    formAssociation
        .getIncompletePrompts()
        .forEach(
            incompletePrompt -> {
              if (!incompletePrompt.getId().equals(formPromptId)) {
                incompletePrompts.add(incompletePrompt);
              }
            });
    formAssociation.setIncompletePrompts(incompletePrompts);
  }

  /** Performs the operation of changing the status of a form prompt from complete to incomplete. */
  private void updatePromptToIncomplete(
      @Nonnull final FormAssociation form,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId) {
    // Remove the prompt from completed.
    final List<FormPromptAssociation> newCompletedPrompts =
        form.getCompletedPrompts().stream()
            .filter(prompt -> !prompt.getId().equals(formPromptId))
            .collect(Collectors.toList());
    form.setCompletedPrompts(new FormPromptAssociationArray(newCompletedPrompts));

    // Add the prompt to in-completed.
    if (form.getIncompletePrompts().stream()
        .anyMatch(prompt -> prompt.getId().equals(formPromptId))) {
      log.warn(
          String.format(
              "Attempting to unset a prompt that is already incomplete. Skipping... Form: %s, Prompt: %s, Entity: %s",
              formUrn, formPromptId, entityUrn));
      return;
    }
    final List<FormPromptAssociation> newIncompletePrompts =
        new ArrayList<>(form.getIncompletePrompts());
    newIncompletePrompts.add(
        new FormPromptAssociation().setId(formPromptId).setLastModified(createSystemAuditStamp()));
    form.setIncompletePrompts(new FormPromptAssociationArray(newIncompletePrompts));
  }

  private List<MetadataChangeProposal> buildAssignFormChanges(
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final Authentication authentication) {
    final List<MetadataChangeProposal> results = new ArrayList<>();
    entityUrns.forEach(
        entityUrn -> {
          try {
            MetadataChangeProposal maybeChange =
                buildAssignFormChange(entityUrn, formUrn, authentication);
            if (maybeChange != null) {
              results.add(maybeChange);
            }
          } catch (Exception e) {
            log.warn(
                String.format(
                    "Failed to retrieve form %s for entity %s. Skipping form assignment",
                    formUrn, entityUrn),
                e);
          }
        });
    return results;
  }

  @Nullable
  private MetadataChangeProposal buildAssignFormChange(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final Authentication authentication)
      throws Exception {

    final EntityResponse response =
        entityClient.getV2(
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(FORMS_ASPECT_NAME),
            authentication);

    Forms formsAspect = new Forms();
    formsAspect.setIncompleteForms(new FormAssociationArray());
    formsAspect.setCompletedForms(new FormAssociationArray());
    if (response != null && response.getAspects().containsKey(FORMS_ASPECT_NAME)) {
      formsAspect = new Forms(response.getAspects().get(FORMS_ASPECT_NAME).getValue().data());
    }

    // if this form is already assigned to this entity, leave it and move on
    Optional<FormAssociation> formAssociation =
        Stream.concat(
                formsAspect.getCompletedForms().stream(), formsAspect.getIncompleteForms().stream())
            .filter(form -> form.getUrn().equals(formUrn))
            .findAny();

    if (formAssociation.isPresent()) {
      return null;
    }

    // add this form to the entity's incomplete form associations.
    FormAssociationArray incompleteForms = formsAspect.getIncompleteForms();
    FormAssociation newAssociation = new FormAssociation();
    newAssociation.setUrn(formUrn);

    // set all prompts as incomplete when assigning this form
    FormInfo formInfo = getFormInfo(formUrn, authentication);
    FormPromptAssociationArray formPromptAssociations = new FormPromptAssociationArray();
    formInfo
        .getPrompts()
        .forEach(
            prompt -> {
              FormPromptAssociation association = new FormPromptAssociation();
              association.setId(prompt.getId());
              association.setLastModified(createAuditStamp(authentication));
              formPromptAssociations.add(association);
            });
    newAssociation.setIncompletePrompts(formPromptAssociations);
    newAssociation.setCompletedPrompts(new FormPromptAssociationArray());
    incompleteForms.add(newAssociation);
    formsAspect.setIncompleteForms(incompleteForms);
    return buildMetadataChangeProposal(entityUrn, FORMS_ASPECT_NAME, formsAspect);
  }

  private List<MetadataChangeProposal> buildUnassignFormChanges(
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final Authentication authentication) {
    final List<MetadataChangeProposal> results = new ArrayList<>();
    entityUrns.forEach(
        entityUrn -> {
          try {
            MetadataChangeProposal maybeChange =
                buildUnassignFormChange(entityUrn, formUrn, authentication);
            if (maybeChange != null) {
              results.add(maybeChange);
            }
          } catch (Exception e) {
            log.warn(
                String.format(
                    "Failed to retrieve form %s for entity %s. Skipping form unassignment.",
                    formUrn, entityUrn),
                e);
          }
        });
    return results;
  }

  @Nullable
  private MetadataChangeProposal buildUnassignFormChange(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final Authentication authentication)
      throws Exception {
    final EntityResponse response =
        entityClient.getV2(
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(FORMS_ASPECT_NAME),
            authentication);
    Forms formsAspect = new Forms();
    formsAspect.setCompletedForms(new FormAssociationArray());
    formsAspect.setIncompleteForms(new FormAssociationArray());
    if (response != null && response.getAspects().containsKey(FORMS_ASPECT_NAME)) {
      formsAspect = new Forms(response.getAspects().get(FORMS_ASPECT_NAME).getValue().data());
    }

    List<FormAssociation> newCompleted =
        new ArrayList<>(
            new FormAssociationArray(
                formsAspect.getCompletedForms().stream()
                    .filter(form -> !form.getUrn().equals(formUrn))
                    .collect(Collectors.toList())));
    List<FormAssociation> newIncomplete =
        new ArrayList<>(
            new FormAssociationArray(
                formsAspect.getIncompleteForms().stream()
                    .filter(form -> !form.getUrn().equals(formUrn))
                    .collect(Collectors.toList())));

    if (newCompleted.size() == formsAspect.getCompletedForms().size()
        && newIncomplete.size() == formsAspect.getIncompleteForms().size()) {
      // No metadata to change. Skip ingestion.
      return null;
    }

    formsAspect.setCompletedForms(new FormAssociationArray(newCompleted));
    formsAspect.setIncompleteForms(new FormAssociationArray(newIncomplete));

    return buildMetadataChangeProposal(entityUrn, FORMS_ASPECT_NAME, formsAspect);
  }

  private List<MetadataChangeProposal> buildUnsetFormPromptChanges(
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final FormInfo formDefinition,
      @Nonnull final Authentication authentication) {
    final List<MetadataChangeProposal> results = new ArrayList<>();
    entityUrns.forEach(
        entityUrn -> {
          try {
            MetadataChangeProposal maybeChange =
                buildUnsetFormPromptChange(
                    entityUrn, formUrn, formPromptId, formDefinition, authentication);
            if (maybeChange != null) {
              results.add(maybeChange);
            }
          } catch (Exception e) {
            log.warn(
                String.format(
                    "Failed to retrieve form %s for entity %s. Skipping form unassignment.",
                    formUrn, entityUrn),
                e);
          }
        });
    return results;
  }

  @Nullable
  private MetadataChangeProposal buildUnsetFormPromptChange(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final FormInfo formDefinition,
      @Nonnull final Authentication authentication)
      throws Exception {

    // Retrieve entity forms state
    final Forms forms = getEntityForms(entityUrn, authentication);

    // First, find the form with the provided urn.
    final FormAssociation formAssociation = getFormWithUrn(forms, formUrn);

    if (formAssociation != null) {
      // 1. Find and mark the provided form prompt as incomplete.
      updatePromptToIncomplete(formAssociation, entityUrn, formUrn, formPromptId);

      // 2. Update the form's completion status given the incomplete prompt.
      updateFormCompletion(forms, formAssociation, formDefinition);

      // 3. Update the form status aspect for the entity.
      return buildMetadataChangeProposal(entityUrn, FORMS_ASPECT_NAME, forms);
    } else {
      // Form not assigned to the entity! Let's warn and do nothing.
      log.warn(
          String.format(
              "Failed to find form with urn %s associated with entity urn %s while attempting to unset form prompt %s. Skipping...",
              formUrn, entityUrn, formPromptId));
    }

    return null;
  }

  private void updateFormCompletion(
      @Nonnull final Forms forms,
      @Nonnull final FormAssociation form,
      @Nonnull final FormInfo formDefinition) {

    final boolean isFormCompleted = isFormCompleted(form, formDefinition);

    if (isFormCompleted) {
      // If the form is complete, we want to add it to completed forms.

      // 1. Remove from incomplete.
      forms.setIncompleteForms(
          new FormAssociationArray(
              forms.getIncompleteForms().stream()
                  .filter(incompleteForm -> !incompleteForm.getUrn().equals(form.getUrn()))
                  .collect(Collectors.toList())));

      // 2. Add to complete (if not already present)
      if (forms.getCompletedForms().stream()
          .noneMatch(completedForm -> completedForm.getUrn().equals(form.getUrn()))) {
        // Not found in completed, let's update it.
        List<FormAssociation> newCompleted = new ArrayList<>(forms.getCompletedForms());
        newCompleted.add(form);
        forms.setCompletedForms(new FormAssociationArray(newCompleted));
      }
    } else {
      // If the form is incomplete, we want to remove it from the completed forms.
      // If the form implies verification, we also ensure that the verification status is
      // un-applied.

      // 1. Remove from complete.
      forms.setCompletedForms(
          new FormAssociationArray(
              forms.getCompletedForms().stream()
                  .filter(completedForm -> !completedForm.getUrn().equals(form.getUrn()))
                  .collect(Collectors.toList())));

      // 2. Add to incomplete (if not already present)
      if (forms.getIncompleteForms().stream()
          .noneMatch(incompleteForm -> incompleteForm.getUrn().equals(form.getUrn()))) {
        // Not found in incompleted. Let's updated
        List<FormAssociation> newIncomplete = new ArrayList<>(forms.getIncompleteForms());
        newIncomplete.add(form);
        forms.setIncompleteForms(new FormAssociationArray(newIncomplete));
      }

      // 3. Remove verification as required.
      if (FormType.VERIFICATION.equals(formDefinition.getType())) {
        removeFormVerification(form.getUrn(), forms);
      }
    }
  }

  /**
   * Returns true if a form is considered completed, false otherwise. This is a function of whether
   * all required prompts are marked as completed.
   *
   * <p>If none or some required prompts are marked as completed, then the form will be considered
   * NOT completed.
   *
   * @param form the form status, as completed for a specific entity.
   * @param formDefinition the form definition, which contains information about which prompts are
   *     required.
   */
  private boolean isFormCompleted(
      @Nonnull final FormAssociation form, @Nonnull final FormInfo formDefinition) {
    final List<String> requiredPromptsIds =
        formDefinition.getPrompts().stream()
            .filter(FormPrompt::isRequired)
            .map(FormPrompt::getId)
            .collect(Collectors.toList());

    final List<String> completedPromptIds =
        form.getCompletedPrompts().stream()
            .map(FormPromptAssociation::getId)
            .collect(Collectors.toList());

    // If all required prompts are completed, then the form is completed.
    return completedPromptIds.containsAll(requiredPromptsIds);
  }

  @Nullable
  private FormAssociation getFormWithUrn(
      @Nonnull final Forms existingForms, @Nonnull final Urn formUrn) {
    // First check in the completed set.
    Optional<FormAssociation> maybeForm =
        existingForms.getCompletedForms().stream()
            .filter(form -> form.getUrn().equals(formUrn))
            .findFirst();
    if (maybeForm.isPresent()) {
      return maybeForm.get();
    }

    // Then check the incomplete set.
    maybeForm =
        existingForms.getIncompleteForms().stream()
            .filter(form -> form.getUrn().equals(formUrn))
            .findFirst();
    if (maybeForm.isPresent()) {
      return maybeForm.get();
    }

    // No form found, return null.
    return null;
  }

  @Nullable
  private FormPromptAssociation getFormPromptAssociation(
      @Nonnull final FormAssociation formAssociation, @Nonnull final String formPromptId) {
    // First check in the completed set.
    Optional<FormPromptAssociation> maybePromptAssociation =
        formAssociation.getCompletedPrompts().stream()
            .filter(prompt -> prompt.getId().equals(formPromptId))
            .findFirst();
    if (maybePromptAssociation.isPresent()) {
      return maybePromptAssociation.get();
    }

    // Then check the incomplete set.
    maybePromptAssociation =
        formAssociation.getIncompletePrompts().stream()
            .filter(prompt -> prompt.getId().equals(formPromptId))
            .findFirst();
    if (maybePromptAssociation.isPresent()) {
      return maybePromptAssociation.get();
    }

    // No prompt association found, return null.
    return null;
  }

  /**
   * Gets a form prompt association by the prompt ID. If none exists (could happen as a form was
   * changed after assigned or some other reason), then create the association and add it to the
   * formAssociation's list of incomplete prompts.
   */
  private FormPromptAssociation getOrDefaultFormPromptAssociation(
      @Nonnull final FormAssociation formAssociation,
      @Nonnull final String formPromptId,
      @Nonnull final Authentication authentication) {
    final FormPromptAssociation existingPromptAssociation =
        getFormPromptAssociation(formAssociation, formPromptId);
    final FormPromptAssociation formPromptAssociation =
        existingPromptAssociation != null ? existingPromptAssociation : new FormPromptAssociation();
    formPromptAssociation.setId(formPromptId);
    formPromptAssociation.setLastModified(
        createAuditStamp(UrnUtils.getUrn(authentication.getActor().toUrnStr())));
    if (existingPromptAssociation == null) {
      FormPromptAssociationArray incompletePrompts =
          new FormPromptAssociationArray(formAssociation.getIncompletePrompts());
      incompletePrompts.add(formPromptAssociation);
      formAssociation.setIncompletePrompts(incompletePrompts);
    }
    return formPromptAssociation;
  }

  private void removeFormVerification(@Nonnull final Urn formUrn, @Nonnull final Forms forms) {
    if (!forms.hasVerifications()) {
      // Nothing to do.
      return;
    }

    // Remove verification of given urn.
    final List<FormVerificationAssociation> newVerifications =
        forms.getVerifications().stream()
            .filter(verification -> !formUrn.equals(verification.getForm()))
            .collect(Collectors.toList());

    // Update verifications for forms aspect.
    forms.setVerifications(new FormVerificationAssociationArray(newVerifications));
  }

  /**
   * A form is assigned to a user if either the user or a group the user is in is explicitly set on
   * the actors field on a form. Otherwise, if the actors field says that owners are assigned,
   * ensure this actor, or a group they're in, is an owner of this entity.
   */
  public boolean isFormAssignedToUser(
      @Nonnull final Urn formUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> groupsForUser,
      @Nonnull final Authentication authentication)
      throws Exception {
    final FormInfo formInfo = getFormInfo(formUrn, authentication);
    final FormActorAssignment formActorAssignment = formInfo.getActors();
    if (FormUtils.isFormAssignedToUser(formActorAssignment, actorUrn, groupsForUser)) {
      return true;
    }

    if (formActorAssignment.isOwners()) {
      Ownership entityOwnership = getEntityOwnership(entityUrn, authentication);
      return OwnershipUtils.isOwnerOfEntity(entityOwnership, actorUrn, groupsForUser);
    }

    return false;
  }

  /**
   * Adds a new form verification association for an entity for this form on their forms aspect. If
   * there was an existing verification association for this form, remove and replace it. First,
   * ensure this form is of VERIFICATION type and that this form is in completedForms.
   */
  public boolean verifyFormForEntity(
      @Nonnull final Urn formUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final Authentication authentication)
      throws Exception {
    final FormInfo formInfo = getFormInfo(formUrn, authentication);
    if (!formInfo.getType().equals(FormType.VERIFICATION)) {
      throw new UnsupportedOperationException(
          String.format("Form %s is not of type VERIFICATION. Cannot verify form.", formUrn));
    }
    final Forms formsAspect = getEntityForms(entityUrn, authentication);
    if (!isFormInCompletedForms(formUrn, formsAspect)) {
      throw new RuntimeException(
          String.format(
              "Form %s is not in the list of completed forms for this entity. Skipping verification.",
              formUrn));
    }

    // Remove any existing verifications for this form to patch a new one
    List<FormVerificationAssociation> formVerifications =
        formsAspect.getVerifications().stream()
            .filter(verification -> !verification.getForm().equals(formUrn))
            .collect(Collectors.toList());
    FormVerificationAssociation newAssociation = new FormVerificationAssociation();
    newAssociation.setForm(formUrn);
    newAssociation.setLastModified(createAuditStamp(authentication));
    formVerifications.add(newAssociation);

    formsAspect.setVerifications(new FormVerificationAssociationArray(formVerifications));

    ingestForms(entityUrn, formsAspect, authentication);
    return true;
  }

  private boolean isFormInCompletedForms(
      @Nonnull final Urn formUrn, @Nonnull final Forms formsAspect) {
    return formsAspect.getCompletedForms().stream()
        .anyMatch(completedForm -> completedForm.getUrn().equals(formUrn));
  }

  public FormInfo getFormInfo(
      @Nonnull final Urn formUrn, @Nonnull final Authentication authentication)
      throws URISyntaxException, RemoteInvocationException {
    final EntityResponse formInfoResponse =
        entityClient.getV2(
            formUrn.getEntityType(),
            formUrn,
            ImmutableSet.of(FORM_INFO_ASPECT_NAME),
            authentication);
    if (formInfoResponse != null
        && formInfoResponse.getAspects().containsKey(FORM_INFO_ASPECT_NAME)) {
      return new FormInfo(
          formInfoResponse.getAspects().get(FORM_INFO_ASPECT_NAME).getValue().data());
    } else {
      throw new RuntimeException(String.format("Form %s does not exist.", formUrn));
    }
  }

  private SchemaMetadata getSchemaMetadata(
      @Nonnull final Urn entityUrn, @Nonnull final Authentication authentication)
      throws URISyntaxException, RemoteInvocationException {
    final EntityResponse response =
        entityClient.getV2(
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(SCHEMA_METADATA_ASPECT_NAME),
            authentication);
    if (response != null && response.getAspects().containsKey(SCHEMA_METADATA_ASPECT_NAME)) {
      return new SchemaMetadata(
          response.getAspects().get(SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    } else {
      throw new RuntimeException(
          String.format("Schema metadata does not exist on entity %s.", entityUrn));
    }
  }

  private Ownership getEntityOwnership(
      @Nonnull final Urn entityUrn, @Nonnull final Authentication authentication)
      throws URISyntaxException, RemoteInvocationException {
    final EntityResponse entityResponse =
        entityClient.getV2(
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(OWNERSHIP_ASPECT_NAME),
            authentication);
    if (entityResponse != null && entityResponse.getAspects().containsKey(OWNERSHIP_ASPECT_NAME)) {
      return new Ownership(
          entityResponse.getAspects().get(OWNERSHIP_ASPECT_NAME).getValue().data());
    } else {
      throw new RuntimeException(String.format("Ownership %s does not exist.", entityUrn));
    }
  }

  private void verifyEntitiesExist(
      @Nonnull final List<Urn> entityUrns, @Nonnull final Authentication authentication) {
    entityUrns.forEach(
        entityUrn -> {
          try {
            verifyEntityExists(entityUrn, authentication);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Issue verifying whether entity exists when assigning form to it. Entity urn: %s",
                    entityUrn));
          }
        });
  }

  private void verifyEntityExists(
      @Nonnull final Urn entityUrn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    if (!entityClient.exists(entityUrn, authentication)) {
      throw new RuntimeException(
          String.format("Entity %s does not exist. Skipping batch form assignment", entityUrn));
    }
  }

  private AuditStamp createSystemAuditStamp() {
    return createAuditStamp(UrnUtils.getUrn(SYSTEM_ACTOR));
  }

  private AuditStamp createAuditStamp(@Nonnull final Authentication authentication) {
    return createAuditStamp(UrnUtils.getUrn(authentication.getActor().toUrnStr()));
  }

  private AuditStamp createAuditStamp(@Nonnull final Urn actor) {
    return new AuditStamp().setTime(System.currentTimeMillis()).setActor(actor);
  }
}
