package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.FORMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.entity.AspectUtils.buildMetadataChangeProposal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.FormType;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.OwnershipUtils;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.FormKey;
import com.linkedin.metadata.query.filter.*;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.util.FormTestBuilder;
import com.linkedin.metadata.service.util.SearchBasedFormAssignmentRunner;
import com.linkedin.metadata.utils.EntityKeyUtils;
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
import com.linkedin.test.TestInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.net.URISyntaxException;
import java.util.*;
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

  private static final int TEST_SEARCH_BATCH_SIZE = 10000;
  private static final int FORMS_BATCH_SIZE = 1000;
  private static final int BATCH_FORM_ENTITY_COUNT = 500;

  public FormService(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(systemEntityClient, openApiClient, objectMapper);
  }

  /** Batch associated a form to a given set of entities by urn. */
  public void batchAssignFormToEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn)
      throws Exception {
    verifyEntityExists(opContext, formUrn);
    verifyEntitiesExist(opContext, entityUrns, false);
    final List<MetadataChangeProposal> changes =
        buildAssignFormChanges(opContext, entityUrns, formUrn);
    ingestChangeProposals(opContext, changes, true);
  }

  /** Batch remove a form from a given entity by urn. */
  public void batchUnassignFormForEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn)
      throws Exception {
    if (!entityClient.exists(opContext, formUrn)) {
      log.warn(String.format("Trying to remove a form with urn %s that does not exist.", formUrn));
    }
    verifyEntitiesExist(opContext, entityUrns, false);
    final List<MetadataChangeProposal> changes =
        buildUnassignFormChanges(opContext, entityUrns, formUrn);
    ingestChangeProposals(opContext, changes, true);
  }

  /** Mark a specific form prompt as incomplete */
  public void batchSetFormPromptIncomplete(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId)
      throws Exception {
    verifyEntityExists(opContext, formUrn);
    verifyEntitiesExist(opContext, entityUrns, false);
    final FormInfo formInfo = getFormInfo(opContext, formUrn);
    final List<MetadataChangeProposal> changes =
        buildUnsetFormPromptChanges(opContext, entityUrns, formUrn, formPromptId, formInfo);
    ingestChangeProposals(opContext, changes, true);
  }

  /** Create a dynamic form assignment for a particular form. */
  public void createDynamicFormAssignment(
      @Nonnull OperationContext opContext,
      @Nonnull final DynamicFormAssignment dynamicFormAssignment,
      @Nonnull final Urn formUrn)
      throws RemoteInvocationException {
    if (!entityClient.exists(opContext, formUrn)) {
      throw new RuntimeException(
          String.format("Form %s does not exist. Skipping dynamic form assignment", formUrn));
    }

    try {
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildMetadataChangeProposal(
              formUrn, Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME, dynamicFormAssignment),
          false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create form", e);
    }
  }

  /**
   * Creates a form prompt automation, which verifies that the prompt is completed, and unsets it
   * when it's not.
   */
  public void upsertFormPromptCompletionAutomation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final FormPrompt prompt) {
    if (prompt.getType().equals(FormPromptType.FIELDS_STRUCTURED_PROPERTY)) {
      log.info(
          "Encountered FIELDS_STRUCTURED_PROPERTY prompt type. Skipping form prompt completion automation");
      return;
    }
    final Urn metadataTestUrn = FormTestBuilder.createTestUrnForFormPrompt(formUrn, prompt);
    final TestInfo testDefinition = FormTestBuilder.buildFormPromptCompletionTest(formUrn, prompt);
    try {
      ingestChangeProposals(
          opContext,
          ImmutableList.of(
              AspectUtils.buildMetadataChangeProposal(
                  metadataTestUrn, TEST_INFO_ASPECT_NAME, testDefinition)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create form", e);
    }
  }

  /** Creates a form assignment automation, which assigns the form to an entity for completion. */
  public void upsertFormAssignmentAutomation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final DynamicFormAssignment formFilters,
      @Nonnull ObjectMapper objectMapper) {
    final Urn metadataTestUrn = FormTestBuilder.createTestUrnForFormAssignment(formUrn);
    final TestInfo testDefinition = FormTestBuilder.buildFormAssignmentTest(formUrn, formFilters);
    try {
      ingestChangeProposals(
          opContext,
          ImmutableList.of(
              AspectUtils.buildMetadataChangeProposal(
                  metadataTestUrn, TEST_INFO_ASPECT_NAME, testDefinition)));
      SearchBasedFormAssignmentRunner.assign(
          opContext,
          formFilters,
          formUrn,
          BATCH_FORM_ENTITY_COUNT,
          entityClient,
          openApiClient,
          objectMapper);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to dynamically assign form with urn: %s", formUrn), e);
    }
  }

  /** Remove all form automations (metadata tests) for a particular form urn. */
  public void removeAllFormAutomations(
      @Nonnull OperationContext opContext, @Nonnull final Urn formUrn) {
    // Lookup all metadata tests associated with this form urn.
    try {
      final SearchResult result =
          this.entityClient.search(
              opContext.withSearchFlags(flags -> flags.setSkipCache(true)),
              TEST_ENTITY_NAME,
              "*",
              ImmutableMap.of("sourceUrn", formUrn.toString()),
              0,
              TEST_SEARCH_BATCH_SIZE);

      if (result.hasEntities()) {
        result
            .getEntities()
            .forEach(
                entity -> {
                  try {
                    this.entityClient.deleteEntity(opContext, entity.getEntity());
                  } catch (Exception e) {
                    throw new RuntimeException("Failed to remove form tests!", e);
                  }
                });
      }
    } catch (Exception e) {
      log.error(
          "Failed to remove metadata tests associated with form urn {}. This may mean that there is stale data remaining!",
          formUrn);
    }
  }

  /** Remove all form automations (metadata tests) for a particular form urn. */
  public void removeFormPromptCompletionAutomation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final FormPrompt prompt) {
    final Urn metadataTestUrn = FormTestBuilder.createTestUrnForFormPrompt(formUrn, prompt);
    try {
      this.entityClient.deleteEntity(opContext, metadataTestUrn);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to remove form prompt test for form %s and prompt id %s!",
              formUrn, prompt.getId()),
          e);
    }
  }

  /** Submit a response for a structured property type prompt. */
  public Boolean batchSubmitStructuredPropertyPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityUrns,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitStructuredPropertyPromptResponse(
                opContext, urn, structuredPropertyUrn, values, formUrn, formPromptId);
          } catch (Exception e) {
            throw new RuntimeException("Failed to batch submit structured property prompt", e);
          }
        });

    return true;
  }

  /** Submit a response for a structured property type prompt. */
  public Boolean submitStructuredPropertyPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId)
      throws Exception {

    // First, let's apply the action and add the structured property.
    ingestStructuredProperties(opContext, entityUrn, structuredPropertyUrn, values);

    // Then, let's apply the change to the entity's form status.
    ingestCompletedFormResponse(opContext, entityUrn, formUrn, formPromptId);

    return true;
  }

  /** Submit a response for a field-level structured property type prompt. */
  public Boolean batchSubmitFieldStructuredPropertyPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityUrns,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final List<String> fieldPaths)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitFieldStructuredPropertyPromptResponse(
                opContext, urn, structuredPropertyUrn, values, formUrn, formPromptId, fieldPaths);
          } catch (Exception e) {
            throw new RuntimeException(
                "Failed to batch submit field structured property prompt", e);
          }
        });

    return true;
  }

  /** Submit a response for a field-level structured property type prompt. */
  public Boolean submitFieldStructuredPropertyPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final List<String> fieldPaths)
      throws Exception {

    // First, let's apply the action and add the structured property.
    for (String fieldPath : fieldPaths) {
      ingestSchemaFieldStructuredProperties(
          opContext, entityUrn, structuredPropertyUrn, values, fieldPath);
    }

    // Then, let's apply the change to the entity's form status.
    ingestCompletedFieldFormResponse(opContext, entityUrn, formUrn, formPromptId, fieldPaths);

    return true;
  }

  private void ingestCompletedFieldFormResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final List<String> fieldPaths)
      throws Exception {
    final Forms forms = getEntityForms(opContext, entityUrn);
    final FormAssociation formAssociation = getFormWithUrn(forms, formUrn);
    if (formAssociation == null) {
      throw new RuntimeException(
          String.format("Form %s has not been assigned to entity %s", formUrn, entityUrn));
    }
    final FormPromptAssociation formPromptAssociation =
        getOrDefaultFormPromptAssociation(opContext, formAssociation, formPromptId);

    // update the prompt association to have this fieldFormPromptAssociation marked as complete
    for (String fieldPath : fieldPaths) {
      updateFieldPromptToComplete(opContext, formPromptAssociation, fieldPath);
    }

    formAssociation.setLastModified(opContext.getAuditStamp());

    // field prompt is complete if all fields in entity's schema metadata are marked complete
    if (isFieldPromptComplete(opContext, entityUrn, formPromptAssociation)) {
      // if this is complete, the prompt as a whole should be marked as complete
      ingestCompletedFormResponse(opContext, entityUrn, formUrn, formPromptId, forms);
    } else {
      // regardless, ingest forms to save state of this aspect
      ingestForms(opContext, entityUrn, forms);
    }
  }

  private void ingestCompletedFormResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId)
      throws Exception {
    final Forms forms = getEntityForms(opContext, entityUrn);
    ingestCompletedFormResponse(opContext, entityUrn, formUrn, formPromptId, forms);
  }

  private void ingestCompletedFormResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final Forms forms)
      throws Exception {
    // Next, get all the information we need to update the forms for the entity.
    final FormInfo formInfo = getFormInfo(opContext, formUrn);
    final FormAssociation formAssociation = getFormWithUrn(forms, formUrn);

    if (formAssociation == null) {
      throw new RuntimeException(
          String.format("Form %s has not been assigned to entity %s", formUrn, entityUrn));
    }

    // First, mark the prompt as completed in forms aspect.
    updatePromptToComplete(opContext, formAssociation, entityUrn, formUrn, formPromptId);

    // Then, update the completed forms fields based on which prompts remain incomplete.
    updateFormCompletion(forms, formAssociation, formInfo);

    // Finally, ingest the newly updated forms aspect.
    ingestForms(opContext, entityUrn, forms);
  }

  private void ingestSchemaFieldStructuredProperties(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final String fieldPath)
      throws Exception {
    Urn schemaFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(entityUrn.toString(), fieldPath);
    ingestStructuredProperties(opContext, schemaFieldUrn, structuredPropertyUrn, values);
  }

  private void ingestStructuredProperties(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values)
      throws Exception {
    final EntityResponse response =
        entityClient.getV2(
            opContext,
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME));

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
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr()))
            .setTime(System.currentTimeMillis()));
    assignment.setLastModified(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr()))
            .setTime(System.currentTimeMillis()));
    filteredAssignments.add(assignment);

    StructuredPropertyValueAssignmentArray assignments =
        new StructuredPropertyValueAssignmentArray(filteredAssignments);
    structuredProperties.setProperties(assignments);

    final MetadataChangeProposal structuredPropertiesProposal =
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);
    try {
      this.entityClient.ingestProposal(opContext, structuredPropertiesProposal, false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to submit form response", e);
    }
  }

  private void ingestForms(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Forms forms) {
    try {
      ingestChangeProposals(
          opContext,
          ImmutableList.of(
              AspectUtils.buildMetadataChangeProposal(entityUrn, FORMS_ASPECT_NAME, forms)));
    } catch (Exception e) {
      log.warn(String.format("Failed to ingest forms for entity with urn %s", entityUrn), e);
    }
  }

  private Forms getEntityForms(@Nonnull OperationContext opContext, @Nonnull final Urn entityUrn)
      throws Exception {
    final EntityResponse response =
        entityClient.getV2(
            opContext, entityUrn.getEntityType(), entityUrn, ImmutableSet.of(FORMS_ASPECT_NAME));
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
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final FormPromptAssociation formPromptAssociation)
      throws Exception {
    final Set<String> completedFieldPaths =
        Objects.requireNonNull(formPromptAssociation.getFieldAssociations())
            .getCompletedFieldPrompts()
            .stream()
            .map(FieldFormPromptAssociation::getFieldPath)
            .collect(Collectors.toSet());
    final SchemaMetadata schemaMetadata = getSchemaMetadata(opContext, entityUrn);
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
      @Nonnull OperationContext opContext,
      @Nonnull final FormPromptAssociation formPromptAssociation,
      @Nonnull final String fieldPath) {
    final FieldFormPromptAssociation completedFieldPromptAssociation =
        new FieldFormPromptAssociation();
    completedFieldPromptAssociation.setFieldPath(fieldPath);
    completedFieldPromptAssociation.setLastModified(opContext.getAuditStamp());

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
      @Nonnull OperationContext opContext,
      @Nonnull final FormAssociation formAssociation,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId) {
    final FormPromptAssociation formPromptAssociation =
        getOrDefaultFormPromptAssociation(opContext, formAssociation, formPromptId);

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

    formAssociation.setLastModified(opContext.getAuditStamp());
  }

  /** Performs the operation of changing the status of a form prompt from complete to incomplete. */
  private void updatePromptToIncomplete(
      @Nonnull OperationContext opContext,
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
        new FormPromptAssociation().setId(formPromptId).setLastModified(opContext.getAuditStamp()));
    form.setIncompletePrompts(new FormPromptAssociationArray(newIncompletePrompts));

    form.setLastModified(opContext.getAuditStamp());
  }

  private List<MetadataChangeProposal> buildAssignFormChanges(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn) {
    final List<MetadataChangeProposal> results = new ArrayList<>();
    entityUrns.forEach(
        entityUrn -> {
          try {
            MetadataChangeProposal maybeChange =
                buildAssignFormChange(opContext, entityUrn, formUrn);
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
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn, @Nonnull final Urn formUrn)
      throws Exception {

    final EntityResponse response =
        entityClient.getV2(
            opContext, entityUrn.getEntityType(), entityUrn, ImmutableSet.of(FORMS_ASPECT_NAME));

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
    FormInfo formInfo = getFormInfo(opContext, formUrn);
    FormPromptAssociationArray formPromptAssociations = new FormPromptAssociationArray();
    formInfo
        .getPrompts()
        .forEach(
            prompt -> {
              FormPromptAssociation association = new FormPromptAssociation();
              association.setId(prompt.getId());
              association.setLastModified(opContext.getAuditStamp());
              formPromptAssociations.add(association);
            });
    newAssociation.setIncompletePrompts(formPromptAssociations);
    newAssociation.setCompletedPrompts(new FormPromptAssociationArray());
    newAssociation.setCreated(opContext.getAuditStamp());
    newAssociation.setLastModified(opContext.getAuditStamp());
    incompleteForms.add(newAssociation);
    formsAspect.setIncompleteForms(incompleteForms);
    return buildMetadataChangeProposal(entityUrn, FORMS_ASPECT_NAME, formsAspect);
  }

  private List<MetadataChangeProposal> buildUnassignFormChanges(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn) {
    final List<MetadataChangeProposal> results = new ArrayList<>();
    entityUrns.forEach(
        entityUrn -> {
          try {
            MetadataChangeProposal maybeChange =
                buildUnassignFormChange(opContext, entityUrn, formUrn);
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
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn, @Nonnull final Urn formUrn)
      throws Exception {
    final EntityResponse response =
        entityClient.getV2(
            opContext, entityUrn.getEntityType(), entityUrn, ImmutableSet.of(FORMS_ASPECT_NAME));
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
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final FormInfo formDefinition) {
    final List<MetadataChangeProposal> results = new ArrayList<>();
    entityUrns.forEach(
        entityUrn -> {
          try {
            MetadataChangeProposal maybeChange =
                buildUnsetFormPromptChange(
                    opContext, entityUrn, formUrn, formPromptId, formDefinition);
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
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final FormInfo formDefinition)
      throws Exception {

    // Retrieve entity forms state
    final Forms forms = getEntityForms(opContext, entityUrn);

    // First, find the form with the provided urn.
    final FormAssociation formAssociation = getFormWithUrn(forms, formUrn);

    if (formAssociation != null) {
      // 1. Find and mark the provided form prompt as incomplete.
      updatePromptToIncomplete(opContext, formAssociation, entityUrn, formUrn, formPromptId);

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
      @Nonnull OperationContext opContext,
      @Nonnull final FormAssociation formAssociation,
      @Nonnull final String formPromptId) {
    final FormPromptAssociation existingPromptAssociation =
        getFormPromptAssociation(formAssociation, formPromptId);
    final FormPromptAssociation formPromptAssociation =
        existingPromptAssociation != null ? existingPromptAssociation : new FormPromptAssociation();
    formPromptAssociation.setId(formPromptId);
    formPromptAssociation.setLastModified(opContext.getAuditStamp());
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
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> groupsForUser)
      throws Exception {
    final FormInfo formInfo = getFormInfo(opContext, formUrn);
    final FormActorAssignment formActorAssignment = formInfo.getActors();
    if (FormUtils.isFormAssignedToUser(formActorAssignment, actorUrn, groupsForUser)) {
      return true;
    }

    if (formActorAssignment.isOwners()) {
      Ownership entityOwnership = getEntityOwnership(opContext, entityUrn);
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
      @Nonnull OperationContext opContext, @Nonnull final Urn formUrn, @Nonnull final Urn entityUrn)
      throws Exception {
    final FormInfo formInfo = getFormInfo(opContext, formUrn);
    if (!formInfo.getType().equals(FormType.VERIFICATION)) {
      throw new UnsupportedOperationException(
          String.format("Form %s is not of type VERIFICATION. Cannot verify form.", formUrn));
    }
    final Forms formsAspect = getEntityForms(opContext, entityUrn);
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
    newAssociation.setLastModified(opContext.getAuditStamp());
    formVerifications.add(newAssociation);

    formsAspect.setVerifications(new FormVerificationAssociationArray(formVerifications));

    ingestForms(opContext, entityUrn, formsAspect);
    return true;
  }

  private boolean isFormInCompletedForms(
      @Nonnull final Urn formUrn, @Nonnull final Forms formsAspect) {
    return formsAspect.getCompletedForms().stream()
        .anyMatch(completedForm -> completedForm.getUrn().equals(formUrn));
  }

  public FormInfo getFormInfo(@Nonnull OperationContext opContext, @Nonnull final Urn formUrn)
      throws URISyntaxException, RemoteInvocationException {
    final EntityResponse formInfoResponse =
        entityClient.getV2(
            opContext, formUrn.getEntityType(), formUrn, ImmutableSet.of(FORM_INFO_ASPECT_NAME));
    if (formInfoResponse != null
        && formInfoResponse.getAspects().containsKey(FORM_INFO_ASPECT_NAME)) {
      return new FormInfo(
          formInfoResponse.getAspects().get(FORM_INFO_ASPECT_NAME).getValue().data());
    } else {
      throw new RuntimeException(String.format("Form %s does not exist.", formUrn));
    }
  }

  private SchemaMetadata getSchemaMetadata(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn)
      throws URISyntaxException, RemoteInvocationException {
    final EntityResponse response =
        entityClient.getV2(
            opContext,
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(SCHEMA_METADATA_ASPECT_NAME));
    if (response != null && response.getAspects().containsKey(SCHEMA_METADATA_ASPECT_NAME)) {
      return new SchemaMetadata(
          response.getAspects().get(SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    } else {
      throw new RuntimeException(
          String.format("Schema metadata does not exist on entity %s.", entityUrn));
    }
  }

  public List<Urn> getGroupsForUser(@Nonnull OperationContext opContext, @Nonnull final Urn userUrn)
      throws Exception {
    final NativeGroupMembership nativeGroupMembership =
        getExistingNativeGroupMembership(opContext, userUrn);
    final GroupMembership groupMembership = getExistingGroupMembership(opContext, userUrn);
    final Set<Urn> allGroups = new HashSet<>();
    allGroups.addAll(nativeGroupMembership.getNativeGroups());
    allGroups.addAll(groupMembership.getGroups());
    return new ArrayList<>(allGroups);
  }

  NativeGroupMembership getExistingNativeGroupMembership(
      @Nonnull OperationContext opContext, @Nonnull final Urn userUrn) throws Exception {
    final EntityResponse entityResponse =
        entityClient
            .batchGetV2(
                opContext,
                CORP_USER_ENTITY_NAME,
                Collections.singleton(userUrn),
                Collections.singleton(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))
            .get(userUrn);

    final NativeGroupMembership nativeGroupMembership;
    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)) {
      // If the user doesn't have the NativeGroupMembership aspect, create one.
      nativeGroupMembership = new NativeGroupMembership();
      nativeGroupMembership.setNativeGroups(new UrnArray());
    } else {
      nativeGroupMembership =
          new NativeGroupMembership(
              entityResponse
                  .getAspects()
                  .get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)
                  .getValue()
                  .data());
    }
    return nativeGroupMembership;
  }

  GroupMembership getExistingGroupMembership(
      @Nonnull OperationContext opContext, @Nonnull final Urn userUrn)
      throws RemoteInvocationException, URISyntaxException {
    final EntityResponse entityResponse =
        entityClient
            .batchGetV2(
                opContext,
                CORP_USER_ENTITY_NAME,
                Collections.singleton(userUrn),
                Collections.singleton(GROUP_MEMBERSHIP_ASPECT_NAME))
            .get(userUrn);

    final GroupMembership groupMembership;
    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(GROUP_MEMBERSHIP_ASPECT_NAME)) {
      // If the user doesn't have the GroupMembership aspect, create one.
      groupMembership = new GroupMembership();
      groupMembership.setGroups(new UrnArray());
    } else {
      groupMembership =
          new GroupMembership(
              entityResponse.getAspects().get(GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
    }
    return groupMembership;
  }

  private Ownership getEntityOwnership(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn)
      throws URISyntaxException, RemoteInvocationException {
    final EntityResponse entityResponse =
        entityClient.getV2(
            opContext,
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(OWNERSHIP_ASPECT_NAME));
    if (entityResponse != null && entityResponse.getAspects().containsKey(OWNERSHIP_ASPECT_NAME)) {
      return new Ownership(
          entityResponse.getAspects().get(OWNERSHIP_ASPECT_NAME).getValue().data());
    } else {
      throw new RuntimeException(String.format("Ownership %s does not exist.", entityUrn));
    }
  }

  private void verifyEntitiesExist(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      boolean shouldThrow) {
    entityUrns.forEach(
        entityUrn -> {
          try {
            verifyEntityExists(opContext, entityUrn);
          } catch (Exception e) {
            log.error(
                String.format(
                    "Entity with urn %s does not exist - could not assign/unassign form",
                    entityUrn));
            if (shouldThrow) {
              throw new RuntimeException(
                  String.format(
                      "Issue verifying whether entity exists when assigning form to it. Entity urn: %s",
                      entityUrn));
            }
          }
        });
  }

  private void verifyEntityExists(@Nonnull OperationContext opContext, @Nonnull final Urn entityUrn)
      throws RemoteInvocationException {
    if (!entityClient.exists(opContext, entityUrn)) {
      throw new RuntimeException(
          String.format("Entity %s does not exist. Skipping batch form assignment", entityUrn));
    }
  }

  /*
   * Get forms that are explicitly assigned to a given actor through either being assigned to
   * the user or one of the groups provided.
   */
  public List<Urn> getFormsAssignedToActor(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn userUrn,
      @Nonnull final List<Urn> groupUrns)
      throws RemoteInvocationException {
    // first, create a filter for the userUrn or the groupUrns matching the form actor assignment
    final CriterionArray assignedUserArray = new CriterionArray();
    assignedUserArray.add(
        new Criterion()
            .setField("assignedUsers")
            .setValue(userUrn.toString())
            .setCondition(Condition.EQUAL));

    List<Urn> uniqueGroupUrns = new HashSet<>(groupUrns).stream().collect(Collectors.toList());
    final CriterionArray assignedGroupsArray = new CriterionArray();
    assignedGroupsArray.add(
        new Criterion()
            .setField("assignedGroups")
            .setValue("")
            .setValues(
                new StringArray(
                    uniqueGroupUrns.stream().map(Urn::toString).collect(Collectors.toList())))
            .setCondition(Condition.EQUAL));

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(assignedUserArray),
                    new ConjunctiveCriterion().setAnd(assignedGroupsArray)));

    SearchResult result =
        this.entityClient.search(
            opContext, FORM_ENTITY_NAME, "*", filter, null, 0, FORMS_BATCH_SIZE);

    return result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
  }

  /*
   * Get forms that are assigned to owners of entities that this form is assigned to.
   */
  public List<Urn> getOwnershipForms(@Nonnull OperationContext opContext)
      throws RemoteInvocationException {
    final SearchResult result =
        this.entityClient.search(
            opContext,
            FORM_ENTITY_NAME,
            "*",
            ImmutableMap.of("isOwnershipForm", "true"),
            0,
            FORMS_BATCH_SIZE);

    return result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
  }

  /*
   * Get forms that are implicitly assigned to this user because the user owns an entity that has an
   * ownership form on it.
   * So, of the ownership forms, return the ones that are on entities that this user owns.
   */
  public List<Urn> getFormsAssignedByOwnership(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull final Urn userUrn,
      @Nonnull final List<Urn> groupUrns,
      @Nonnull final List<Urn> ownershipFormUrns)
      throws RemoteInvocationException {
    Criterion formOwnershipCriterion = FormUtils.getFormOwnershipCriterion(userUrn, groupUrns);

    // create filter for entities owned by this user
    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(formOwnershipCriterion))));

    // do an aggregations query to get all the form urns in completedForms and incompleteForms for
    // entities that this user owns
    List<String> formFacets = ImmutableList.of("completedForms", "incompleteForms");
    SearchResult result =
        this.entityClient.searchAcrossEntities(
            opContext,
            entities,
            "*",
            filter,
            0,
            0, // doing an aggregate request here, count = 0
            Collections.emptyList(),
            formFacets,
            null);

    // get the form urns from the aggregations query we make above
    final Set<Urn> formsOnOwnedEntities = new HashSet<>();
    result
        .getMetadata()
        .getAggregations()
        .forEach(
            agg -> {
              if (formFacets.contains(agg.getName())) {
                formsOnOwnedEntities.addAll(
                    agg.getFilterValues().stream()
                        .map(f -> UrnUtils.getUrn(f.getValue()))
                        .collect(Collectors.toList()));
              }
            });

    return ownershipFormUrns.stream()
        .filter(formsOnOwnedEntities::contains)
        .collect(Collectors.toList());
  }

  /** Create a form given the formInfo aspect. */
  public Urn createForm(
      @Nonnull OperationContext opContext,
      @Nonnull final FormInfo formInfo,
      @Nullable final String id) {

    FormKey formKey = new FormKey();
    String formId = id != null ? id : UUID.randomUUID().toString();
    formKey.setId(formId);
    Urn formUrn = EntityKeyUtils.convertEntityKeyToUrn(formKey, FORM_ENTITY_NAME);

    try {
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildMetadataChangeProposal(formUrn, FORM_INFO_ASPECT_NAME, formInfo),
          false);
      return formUrn;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create form", e);
    }
  }
}
