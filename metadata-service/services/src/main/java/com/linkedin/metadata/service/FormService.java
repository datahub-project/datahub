package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.FORMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORM_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.entity.AspectUtils.buildMetadataChangeProposal;
import static com.linkedin.metadata.service.util.MetadataTestServiceUtils.applyAppSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
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
import com.linkedin.metadata.aspect.patch.builder.FormsPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder;
import com.linkedin.metadata.authorization.OwnershipUtils;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.FormKey;
import com.linkedin.metadata.query.filter.*;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.util.FormTestBuilder;
import com.linkedin.metadata.service.util.SearchBasedFormAssignmentRunner;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.FormUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.metadata.utils.elasticsearch.AcrylSearchUtils;
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
import java.util.UUID;
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
  private static final List<FormPromptType> FIELD_LEVEL_PROMPT_TYPES =
      ImmutableList.of(
          FormPromptType.FIELDS_STRUCTURED_PROPERTY,
          FormPromptType.FIELDS_DOCUMENTATION,
          FormPromptType.FIELDS_GLOSSARY_TERMS);

  private final OwnerService _ownerService;
  private final GlossaryTermService _glossaryTermService;
  private final DocumentationService _documentationService;
  private final DomainService _domainService;
  private final String _appSource;
  private final boolean _isAsync;

  public FormService(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      @Nonnull final String appSource,
      final boolean isAsync) {
    super(systemEntityClient, openApiClient, objectMapper);
    _ownerService = new OwnerService(systemEntityClient, openApiClient, objectMapper, isAsync);
    _glossaryTermService =
        new GlossaryTermService(systemEntityClient, openApiClient, objectMapper, isAsync);
    _documentationService =
        new DocumentationService(
            systemEntityClient, openApiClient, objectMapper, appSource, isAsync);
    _domainService = new DomainService(systemEntityClient, openApiClient, objectMapper, isAsync);
    _appSource = appSource;
    _isAsync = isAsync;
  }

  public FormService(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(systemEntityClient, openApiClient, objectMapper);
    _ownerService = new OwnerService(systemEntityClient, openApiClient, objectMapper);
    _glossaryTermService = new GlossaryTermService(systemEntityClient, openApiClient, objectMapper);
    _documentationService =
        new DocumentationService(systemEntityClient, openApiClient, objectMapper);
    _domainService = new DomainService(systemEntityClient, openApiClient, objectMapper);
    _appSource = null;
    _isAsync = false;
  }

  /** Batch associated a form to a given set of entities by urn. */
  public void batchAssignFormToEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn) {
    verifyEntityExists(opContext, formUrn);
    verifyEntitiesExist(opContext, entityUrns, false);
    final List<MetadataChangeProposal> changes =
        buildAssignFormChanges(opContext, entityUrns, formUrn);
    if (_appSource != null) {
      applyAppSource(changes, _appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
  }

  /** Batch remove a form from a given entity by urn. */
  public void batchUnassignFormForEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> entityUrns,
      @Nonnull final Urn formUrn) {
    verifyEntityExists(opContext, formUrn);
    verifyEntitiesExist(opContext, entityUrns, false);
    final List<MetadataChangeProposal> changes =
        buildUnassignFormChanges(opContext, entityUrns, formUrn);
    if (_appSource != null) {
      applyAppSource(changes, _appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
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
    if (_appSource != null) {
      applyAppSource(changes, _appSource);
    }
    ingestChangeProposals(opContext, changes, _isAsync);
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
      @Nonnull final List<FormPrompt> prompts) {
    List<MetadataChangeProposal> changes = new ArrayList<>();
    for (FormPrompt prompt : prompts) {
      if (FIELD_LEVEL_PROMPT_TYPES.contains(prompt.getType())) {
        log.info("Encountered field level prompt type. Skipping form prompt completion automation");
        return;
      }
      final Urn metadataTestUrn = FormTestBuilder.createTestUrnForFormPrompt(formUrn, prompt);
      final TestInfo testDefinition =
          FormTestBuilder.buildFormPromptCompletionTest(opContext, formUrn, prompt);
      changes.add(
          AspectUtils.buildMetadataChangeProposal(
              metadataTestUrn, TEST_INFO_ASPECT_NAME, testDefinition));
    }
    try {
      if (_appSource != null) {
        applyAppSource(changes, _appSource);
      }
      ingestChangeProposals(opContext, changes, _isAsync);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create form", e);
    }
  }

  /** Creates a form assignment automation, which assigns the form to an entity for completion. */
  public Thread upsertFormAssignmentAutomation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final DynamicFormAssignment formFilters) {
    final Urn metadataTestUrn = FormTestBuilder.createTestUrnForFormAssignment(formUrn);
    final TestInfo testDefinition =
        FormTestBuilder.buildFormAssignmentTest(opContext, formUrn, formFilters);
    try {
      List<MetadataChangeProposal> changes =
          ImmutableList.of(
              AspectUtils.buildMetadataChangeProposal(
                  metadataTestUrn, TEST_INFO_ASPECT_NAME, testDefinition));
      if (_appSource != null) {
        applyAppSource(changes, _appSource);
      }
      ingestChangeProposals(opContext, changes, _isAsync);
      Predicate predicate = AcrylSearchUtils.convertFilterToPredicate(formFilters.getFilter());

      String predicateJson = opContext.getObjectMapper().writeValueAsString(predicate);

      return SearchBasedFormAssignmentRunner.assign(
          opContext, predicateJson, formUrn, BATCH_FORM_ENTITY_COUNT, entityClient, openApiClient);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to dynamically assign form with urn: %s", formUrn), e);
    }
  }

  public Thread removeFormAssignmentAutomation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final DynamicFormAssignment formFilters) {
    try {
      // Remove assignments from entities that do not match the form filter, but do have the form
      // assigned
      Predicate notFilters =
          Predicate.of(
              OperatorType.NOT,
              Collections.singletonList(
                  AcrylSearchUtils.convertFilterToPredicate(formFilters.getFilter())));

      StringListLiteral formUrnLiteral =
          new StringListLiteral(Collections.singletonList(formUrn.toString()));
      Query incompleteFormsQuery = new Query("incompleteForms");
      Predicate incompleteForms =
          Predicate.of(
              OperatorType.ANY_EQUALS, ImmutableList.of(incompleteFormsQuery, formUrnLiteral));

      Query completedFormsQuery = new Query("completedForms");
      Predicate completedForms =
          Predicate.of(
              OperatorType.ANY_EQUALS, ImmutableList.of(completedFormsQuery, formUrnLiteral));

      Query verifiedFormsQuery = new Query("verifiedForms");
      Predicate verifiedForms =
          Predicate.of(
              OperatorType.ANY_EQUALS, ImmutableList.of(verifiedFormsQuery, formUrnLiteral));

      Predicate formsPredicate =
          Predicate.of(
              OperatorType.OR, ImmutableList.of(incompleteForms, completedForms, verifiedForms));

      Predicate finalPredicate =
          Predicate.of(OperatorType.AND, ImmutableList.of(notFilters, formsPredicate));

      String predicateJson = opContext.getObjectMapper().writeValueAsString(finalPredicate);
      return SearchBasedFormAssignmentRunner.unassign(
          opContext, predicateJson, formUrn, BATCH_FORM_ENTITY_COUNT, entityClient, openApiClient);
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
      @Nonnull final String formPromptId,
      @Nullable final Urn actorUrn,
      @Nonnull final boolean shouldThrow)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitStructuredPropertyPromptResponse(
                opContext, urn, structuredPropertyUrn, values, formUrn, formPromptId, actorUrn);
          } catch (Exception e) {
            log.error("Failed to batch submit structured property prompt", e);
            if (shouldThrow) {
              throw new RuntimeException("Failed to batch submit structured property prompt", e);
            }
          }
        });

    return true;
  }

  /** Submit a response for an ownership type prompt. */
  public Boolean batchSubmitOwnershipPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityUrns,
      @Nonnull final List<Urn> owners,
      @Nonnull Urn ownershipTypeUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn,
      final boolean shouldThrow)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitOwnershipPromptResponse(
                opContext, urn, owners, ownershipTypeUrn, formUrn, formPromptId, actorUrn);
          } catch (Exception e) {
            log.error("Failed to batch submit structured property prompt", e);
            if (shouldThrow) {
              throw new RuntimeException("Failed to batch submit structured property prompt", e);
            }
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
      @Nonnull final String formPromptId,
      @Nullable final Urn actorUrn)
      throws Exception {

    // First, let's apply the action and add the structured property.
    ingestStructuredProperties(opContext, entityUrn, structuredPropertyUrn, values, actorUrn);

    // Then, let's apply the change to the entity's form status.
    FormPromptResponse promptResponse = new FormPromptResponse();
    StructuredPropertyPromptResponse propertyResponse = new StructuredPropertyPromptResponse();
    propertyResponse.setPropertyUrn(structuredPropertyUrn);
    propertyResponse.setValues(values);
    promptResponse.setStructuredPropertyResponse(propertyResponse);
    ingestCompletedFormResponse(
        opContext, entityUrn, formUrn, formPromptId, promptResponse, actorUrn);

    return true;
  }

  /** Submit a response for a structured property type prompt. */
  public Boolean submitOwnershipPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> owners,
      @Nonnull Urn ownershipTypeUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn)
      throws Exception {

    // First, let's apply the action and add the owners
    ResourceReference resourceRef = new ResourceReference(entityUrn, null, null);
    final String finalAppSource = _appSource != null ? _appSource : UI_SOURCE;
    // do we pass in _isAsync or take the same approach with ownerService and set as class var?
    _ownerService.batchAddOwners(
        opContext,
        owners,
        ImmutableList.of(resourceRef),
        OwnershipType.NONE,
        ownershipTypeUrn,
        finalAppSource,
        actorUrn);

    // Then, let's apply the change to the entity's form status.
    FormPromptResponse promptResponse = new FormPromptResponse();
    OwnershipPromptResponse ownershipResponse = new OwnershipPromptResponse();
    ownershipResponse.setOwners(new UrnArray(owners));
    ownershipResponse.setOwnershipTypeUrn(ownershipTypeUrn);
    promptResponse.setOwnershipResponse(ownershipResponse);
    ingestCompletedFormResponse(
        opContext, entityUrn, formUrn, formPromptId, promptResponse, actorUrn);

    return true;
  }

  /** Submit a response for a documentation type prompt. */
  public Boolean batchSubmitDocumentationPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityUrns,
      @Nonnull final String documentation,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn,
      final boolean shouldThrow)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitDocumentationPromptResponse(
                opContext, urn, documentation, formUrn, formPromptId, actorUrn);
          } catch (Exception e) {
            log.error("Failed to batch submit documentation prompt", e);
            if (shouldThrow) {
              throw new RuntimeException("Failed to batch submit documentation prompt", e);
            }
          }
        });

    return true;
  }

  /** Submit a response for a documentation type prompt. */
  public Boolean submitDocumentationPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String documentation,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn)
      throws Exception {

    // First, let's update documentation
    _documentationService.updateDocumentation(opContext, entityUrn, documentation, actorUrn);

    // Then, let's apply the change to the entity's form status.
    FormPromptResponse promptResponse = createDocumentationPromptResponse(documentation);
    ingestCompletedFormResponse(
        opContext, entityUrn, formUrn, formPromptId, promptResponse, actorUrn);

    return true;
  }

  /** Submit a response for a field-level Documentation type prompt. */
  public Boolean submitFieldDocumentationPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String documentation,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final List<String> fieldPaths,
      @Nullable final Urn actorUrn)
      throws Exception {

    // First, let's apply the action and add the structured property.
    _documentationService.updateSchemaFieldsDocumentation(
        opContext, entityUrn, fieldPaths, documentation, actorUrn);

    // Then, let's apply the change to the entity's form status.
    FormPromptResponse promptResponse = createDocumentationPromptResponse(documentation);
    ingestCompletedFieldFormResponse(
        opContext, entityUrn, formUrn, formPromptId, fieldPaths, actorUrn, promptResponse);

    return true;
  }

  private FormPromptResponse createDocumentationPromptResponse(
      @Nonnull final String documentation) {
    FormPromptResponse promptResponse = new FormPromptResponse();
    DocumentationPromptResponse documentationResponse = new DocumentationPromptResponse();
    documentationResponse.setDocumentation(documentation);
    promptResponse.setDocumentationResponse(documentationResponse);
    return promptResponse;
  }

  /** Batch submit a response for a glossary terms type prompt. */
  public Boolean batchSubmitGlossaryTermsPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityUrns,
      @Nonnull final List<Urn> glossaryTerms,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn,
      final boolean shouldThrow)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitGlossaryTermsPromptResponse(
                opContext, urn, glossaryTerms, formUrn, formPromptId, actorUrn);
          } catch (Exception e) {
            log.error("Failed to batch submit structured property prompt", e);
            if (shouldThrow) {
              throw new RuntimeException("Failed to batch submit structured property prompt", e);
            }
          }
        });

    return true;
  }

  /** Submit a response for a glossary terms type prompt. */
  public Boolean submitGlossaryTermsPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> glossaryTerms,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn)
      throws Exception {

    // First, let's apply the action and add the terms
    ResourceReference resourceRef = new ResourceReference(entityUrn, null, null);
    final String finalAppSource = _appSource != null ? _appSource : UI_SOURCE;
    _glossaryTermService.batchAddGlossaryTerms(
        opContext, glossaryTerms, ImmutableList.of(resourceRef), finalAppSource, actorUrn);

    // Then, let's apply the change to the entity's form status.
    FormPromptResponse promptResponse = createGlossaryTermsPromptResponse(glossaryTerms);
    ingestCompletedFormResponse(
        opContext, entityUrn, formUrn, formPromptId, promptResponse, actorUrn);

    return true;
  }

  /** Submit a response for a field-level Documentation type prompt. */
  public Boolean submitFieldGlossaryTermsPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<Urn> glossaryTerms,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final List<String> fieldPaths,
      @Nullable final Urn actorUrn)
      throws Exception {

    // First, let's apply the action and add the terms
    List<ResourceReference> resourceRefs = new ArrayList<>();
    fieldPaths.forEach(
        fieldPath -> {
          resourceRefs.add(
              new ResourceReference(entityUrn, SubResourceType.DATASET_FIELD, fieldPath));
        });
    final String finalAppSource = _appSource != null ? _appSource : UI_SOURCE;
    _glossaryTermService.batchAddGlossaryTerms(
        opContext, glossaryTerms, resourceRefs, finalAppSource, actorUrn);

    // Then, let's apply the change to the entity's form status.
    FormPromptResponse promptResponse = createGlossaryTermsPromptResponse(glossaryTerms);
    ingestCompletedFieldFormResponse(
        opContext, entityUrn, formUrn, formPromptId, fieldPaths, actorUrn, promptResponse);

    return true;
  }

  private FormPromptResponse createGlossaryTermsPromptResponse(
      @Nonnull final List<Urn> glossaryTerms) {
    FormPromptResponse promptResponse = new FormPromptResponse();
    GlossaryTermsPromptResponse termsPromptResponse = new GlossaryTermsPromptResponse();
    UrnArray termUrns = new UrnArray(glossaryTerms);
    termsPromptResponse.setGlossaryTerms(termUrns);
    promptResponse.setGlossaryTermsResponse(termsPromptResponse);
    return promptResponse;
  }

  /** Batch submit a response for a glossary terms type prompt. */
  public Boolean batchSubmitDomainPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityUrns,
      @Nonnull final Urn domainUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn,
      final boolean shouldThrow)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitDomainPromptResponse(opContext, urn, domainUrn, formUrn, formPromptId, actorUrn);
          } catch (Exception e) {
            log.error("Failed to batch submit domain prompt", e);
            if (shouldThrow) {
              throw new RuntimeException("Failed to batch submit domain prompt", e);
            }
          }
        });

    return true;
  }

  /** Submit a response for a domain type prompt. */
  public Boolean submitDomainPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn domainUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn)
      throws Exception {

    // First, let's apply the action and set the domain
    ResourceReference resourceRef = new ResourceReference(entityUrn, null, null);
    final String finalAppSource = _appSource != null ? _appSource : UI_SOURCE;
    _domainService.batchSetDomain(
        opContext, domainUrn, ImmutableList.of(resourceRef), finalAppSource);

    // Then, let's apply the change to the entity's form status.
    FormPromptResponse promptResponse = createDomainPromptResponse(domainUrn);
    ingestCompletedFormResponse(
        opContext, entityUrn, formUrn, formPromptId, promptResponse, actorUrn);

    return true;
  }

  private FormPromptResponse createDomainPromptResponse(@Nonnull final Urn domainUrn) {
    FormPromptResponse promptResponse = new FormPromptResponse();
    DomainPromptResponse domainPromptResponse = new DomainPromptResponse();
    domainPromptResponse.setDomain(domainUrn);
    promptResponse.setDomainResponse(domainPromptResponse);
    return promptResponse;
  }

  /** Submit a response for a field-level structured property type prompt. */
  public Boolean batchSubmitFieldStructuredPropertyPromptResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityUrns,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final List<String> fieldPaths,
      @Nullable final Urn actorUrn)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            submitFieldStructuredPropertyPromptResponse(
                opContext,
                urn,
                structuredPropertyUrn,
                values,
                formUrn,
                formPromptId,
                fieldPaths,
                actorUrn);
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
      @Nonnull final List<String> fieldPaths,
      @Nullable final Urn actorUrn)
      throws Exception {

    // First, let's apply the action and add the structured property.
    for (String fieldPath : fieldPaths) {
      ingestSchemaFieldStructuredProperties(
          opContext, entityUrn, structuredPropertyUrn, values, fieldPath, actorUrn);
    }

    // Then, let's apply the change to the entity's form status.
    FormPromptResponse promptResponse =
        createStructuredPropertiesPromptResponse(structuredPropertyUrn, values);
    ingestCompletedFieldFormResponse(
        opContext, entityUrn, formUrn, formPromptId, fieldPaths, actorUrn, promptResponse);

    return true;
  }

  private FormPromptResponse createStructuredPropertiesPromptResponse(
      @Nonnull final Urn structuredPropertyUrn, @Nonnull final PrimitivePropertyValueArray values) {
    FormPromptResponse promptResponse = new FormPromptResponse();
    StructuredPropertyPromptResponse propertyResponse = new StructuredPropertyPromptResponse();
    propertyResponse.setPropertyUrn(structuredPropertyUrn);
    propertyResponse.setValues(values);
    promptResponse.setStructuredPropertyResponse(propertyResponse);
    return promptResponse;
  }

  private void ingestCompletedFieldFormResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final List<String> fieldPaths,
      @Nullable Urn actorUrn,
      @Nonnull final FormPromptResponse promptResponse)
      throws Exception {
    final Forms forms = getEntityForms(opContext, entityUrn);
    final FormAssociation formAssociation = getFormWithUrn(forms, formUrn);
    if (formAssociation == null) {
      throw new RuntimeException(
          String.format("Form %s has not been assigned to entity %s", formUrn, entityUrn));
    }
    final FormPromptAssociation formPromptAssociation =
        getOrDefaultFormPromptAssociation(opContext, formAssociation, formPromptId, actorUrn);

    // update the prompt association to have this fieldFormPromptAssociation marked as complete
    for (String fieldPath : fieldPaths) {
      updateFieldPromptToComplete(opContext, formPromptAssociation, fieldPath, promptResponse);
    }

    formAssociation.setLastModified(opContext.getAuditStamp());

    // field prompt is complete if all fields in entity's schema metadata are marked complete
    if (isFieldPromptComplete(opContext, entityUrn, formPromptAssociation)) {
      // if this is complete, the prompt as a whole should be marked as complete
      ingestCompletedFormResponse(
          opContext, entityUrn, formUrn, formPromptId, forms, null, actorUrn);
    } else {
      // regardless, ingest forms to save state of this aspect
      ingestForms(opContext, entityUrn, forms);
    }
  }

  private void ingestCompletedFormResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable final FormPromptResponse promptResponse,
      @Nullable Urn actorUrn)
      throws Exception {
    // TODO: determine if we want to do this. it may be nice in non-async cases to update forms
    // while updated prompts right away instead of waiting for MCL hook to update forms
    if (_appSource != null && _appSource.equals(METADATA_TESTS_SOURCE)) {
      patchCompletedFormResponse(
          opContext, entityUrn, formUrn, formPromptId, promptResponse, actorUrn);
    } else {
      final Forms forms = getEntityForms(opContext, entityUrn);
      ingestCompletedFormResponse(
          opContext, entityUrn, formUrn, formPromptId, forms, promptResponse, actorUrn);
    }
  }

  private void patchCompletedFormResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nullable final FormPromptResponse promptResponse,
      @Nullable Urn actorUrn)
      throws Exception {
    FormPromptAssociation promptAssociation = new FormPromptAssociation();
    promptAssociation.setId(formPromptId);
    final Urn finalActorUrn = getActorUrn(opContext, actorUrn);
    promptAssociation.setLastModified(
        new AuditStamp().setActor(finalActorUrn).setTime(System.currentTimeMillis()));
    promptAssociation.setResponse(promptResponse, SetMode.IGNORE_NULL);

    MetadataChangeProposal formInfoPatch =
        new FormsPatchBuilder().urn(entityUrn).completePrompt(formUrn, promptAssociation).build();
    ingestChangeProposals(opContext, ImmutableList.of(formInfoPatch), _isAsync);
  }

  private void ingestCompletedFormResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nonnull final String formPromptId,
      @Nonnull final Forms forms,
      @Nullable final FormPromptResponse promptResponse,
      @Nullable Urn actorUrn)
      throws Exception {
    // Next, get all the information we need to update the forms for the entity.
    final FormInfo formInfo = getFormInfo(opContext, formUrn);
    final FormAssociation formAssociation = getFormWithUrn(forms, formUrn);

    if (formAssociation == null) {
      throw new RuntimeException(
          String.format("Form %s has not been assigned to entity %s", formUrn, entityUrn));
    }

    // First, mark the prompt as completed in forms aspect.
    updatePromptToComplete(
        opContext, formAssociation, entityUrn, formUrn, formPromptId, promptResponse, actorUrn);

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
      @Nonnull final String fieldPath,
      @Nullable final Urn actorUrn)
      throws Exception {
    Urn schemaFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(entityUrn, fieldPath);
    ingestStructuredProperties(opContext, schemaFieldUrn, structuredPropertyUrn, values, actorUrn);
  }

  private void ingestStructuredProperties(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nullable final Urn actorUrn)
      throws Exception {
    final Urn finalActorUrn = getActorUrn(opContext, actorUrn);
    AuditStamp lastModified =
        new AuditStamp().setActor(finalActorUrn).setTime(System.currentTimeMillis());

    if (_appSource != null && _appSource.equals(METADATA_TESTS_SOURCE)) {
      patchStructuredProperties(opContext, entityUrn, structuredPropertyUrn, values, lastModified);
      return;
    }

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
        new AuditStamp().setActor(finalActorUrn).setTime(System.currentTimeMillis()));
    assignment.setLastModified(
        new AuditStamp().setActor(finalActorUrn).setTime(System.currentTimeMillis()));
    filteredAssignments.add(assignment);

    StructuredPropertyValueAssignmentArray assignments =
        new StructuredPropertyValueAssignmentArray(filteredAssignments);
    structuredProperties.setProperties(assignments);

    final MetadataChangeProposal structuredPropertiesProposal =
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);

    if (_appSource != null) {
      applyAppSource(ImmutableList.of(structuredPropertiesProposal), _appSource);
    }
    try {
      this.entityClient.ingestProposal(opContext, structuredPropertiesProposal, _isAsync);
    } catch (Exception e) {
      throw new RuntimeException("Failed to submit form response", e);
    }
  }

  private void patchStructuredProperties(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final PrimitivePropertyValueArray values,
      @Nonnull final AuditStamp lastModified)
      throws Exception {
    MetadataChangeProposal patchMcp =
        new StructuredPropertiesPatchBuilder()
            .urn(entityUrn)
            .setProperty(structuredPropertyUrn, values, lastModified)
            .build();
    ingestChangeProposals(opContext, ImmutableList.of(patchMcp), _isAsync);
  }

  private void ingestForms(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Forms forms) {
    try {
      MetadataChangeProposal mcp =
          AspectUtils.buildMetadataChangeProposal(entityUrn, FORMS_ASPECT_NAME, forms);
      if (_appSource != null) {
        applyAppSource(ImmutableList.of(mcp), _appSource);
      }
      ingestChangeProposals(opContext, ImmutableList.of(mcp), _isAsync);
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
      @Nonnull final String fieldPath,
      @Nonnull final FormPromptResponse promptResponse) {
    final FieldFormPromptAssociation completedFieldPromptAssociation =
        new FieldFormPromptAssociation();
    completedFieldPromptAssociation.setFieldPath(fieldPath);
    completedFieldPromptAssociation.setLastModified(opContext.getAuditStamp());
    completedFieldPromptAssociation.setResponse(promptResponse);

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
      @Nonnull final String formPromptId,
      @Nullable final FormPromptResponse promptResponse,
      @Nullable Urn actorUrn) {
    final FormPromptAssociation formPromptAssociation =
        getOrDefaultFormPromptAssociation(opContext, formAssociation, formPromptId, actorUrn);
    if (promptResponse != null) {
      formPromptAssociation.setResponse(promptResponse);
    }

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

    final Urn finalActorUrn = getActorUrn(opContext, actorUrn);
    formAssociation.setLastModified(
        new AuditStamp().setActor(finalActorUrn).setTime(System.currentTimeMillis()));
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

  @VisibleForTesting
  List<MetadataChangeProposal> buildAssignFormChanges(
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
            // added due to test case expecting this
            throw new RuntimeException(e);
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

  @VisibleForTesting
  List<MetadataChangeProposal> buildUnassignFormChanges(
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
  public boolean isFormCompleted(
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
      @Nonnull final String formPromptId,
      @Nullable Urn actorUrn) {
    final FormPromptAssociation existingPromptAssociation =
        getFormPromptAssociation(formAssociation, formPromptId);
    final FormPromptAssociation formPromptAssociation =
        existingPromptAssociation != null ? existingPromptAssociation : new FormPromptAssociation();
    formPromptAssociation.setId(formPromptId);
    final Urn finalActorUrn = getActorUrn(opContext, actorUrn);
    formPromptAssociation.setLastModified(
        new AuditStamp().setActor(finalActorUrn).setTime(System.currentTimeMillis()));
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

  /** Verifies a form for all the provided entities */
  public void batchVerifyForm(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityUrns,
      @Nonnull final Urn formUrn,
      @Nullable final Urn actorUrn)
      throws Exception {
    entityUrns.forEach(
        urnStr -> {
          Urn urn = UrnUtils.getUrn(urnStr);
          try {
            verifyFormForEntity(opContext, formUrn, urn, actorUrn);
          } catch (Exception e) {
            log.error(String.format("Failed to verify form for entity %s", urn), e);
          }
        });
  }

  /**
   * Adds a new form verification association for an entity for this form on their forms aspect. If
   * there was an existing verification association for this form, remove and replace it. First,
   * ensure this form is of VERIFICATION type and that this form is in completedForms.
   */
  public boolean verifyFormForEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final Urn entityUrn,
      @Nullable Urn actorUrn)
      throws Exception {
    if (_appSource != null && _appSource.equals(METADATA_TESTS_SOURCE)) {
      patchVerifyForm(opContext, entityUrn, formUrn, actorUrn);
      return true;
    }

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
    final Urn finalActorUrn = getActorUrn(opContext, actorUrn);
    newAssociation.setLastModified(
        new AuditStamp().setActor(finalActorUrn).setTime(System.currentTimeMillis()));
    formVerifications.add(newAssociation);

    formsAspect.setVerifications(new FormVerificationAssociationArray(formVerifications));

    ingestForms(opContext, entityUrn, formsAspect);
    return true;
  }

  private void patchVerifyForm(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn formUrn,
      @Nullable Urn actorUrn)
      throws Exception {
    FormVerificationAssociation verificationAssociation = new FormVerificationAssociation();
    verificationAssociation.setForm(formUrn);
    final Urn finalActorUrn = getActorUrn(opContext, actorUrn);
    verificationAssociation.setLastModified(
        new AuditStamp().setActor(finalActorUrn).setTime(System.currentTimeMillis()));

    MetadataChangeProposal formsPatch =
        new FormsPatchBuilder().urn(entityUrn).verifyForm(verificationAssociation).build();
    ingestChangeProposals(opContext, ImmutableList.of(formsPatch), _isAsync);
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

  private void verifyEntityExists(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      if (!entityClient.exists(opContext, entityUrn)) {
        throw new RuntimeException(
            String.format("Entity %s does not exist. Skipping batch form assignment", entityUrn));
      }
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(e);
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

  public Urn getActorUrn(@Nonnull OperationContext opContext, @Nullable Urn actorUrn) {
    return actorUrn != null
        ? actorUrn
        : UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr());
  }

  public Map<Urn, FormInfo> batchFetchForms(
      @Nonnull OperationContext opContext, @Nonnull final Set<Urn> urns) {
    try {
      Map<Urn, EntityResponse> batchResponse =
          this.entityClient.batchGetV2(
              opContext, FORM_ENTITY_NAME, urns, ImmutableSet.of(FORM_INFO_ASPECT_NAME));
      Map<Urn, FormInfo> formInfoMap = new HashMap<>();
      for (Map.Entry<Urn, EntityResponse> entry : batchResponse.entrySet()) {
        if (entry.getValue().getAspects().containsKey(FORM_INFO_ASPECT_NAME)) {
          FormInfo formInfo =
              new FormInfo(
                  entry.getValue().getAspects().get(FORM_INFO_ASPECT_NAME).getValue().data());
          formInfoMap.put(entry.getKey(), formInfo);
        }
      }
      return formInfoMap;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to batch fetch forms with urns %s", urns), e);
    }
  }
}
