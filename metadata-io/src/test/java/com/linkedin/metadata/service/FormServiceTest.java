package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormPromptAssociationArray;
import com.linkedin.common.FormVerificationAssociation;
import com.linkedin.common.FormVerificationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DomainParams;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.FormType;
import com.linkedin.form.GlossaryTermsParams;
import com.linkedin.form.OwnershipParams;
import com.linkedin.form.PromptCardinality;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.config.TestsHookExecutionLimitConfiguration;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.util.FormTestBuilder;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.TestFetcher;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.EntityUrnTypeEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.test.query.QueryVersionedAspectEvaluator;
import com.linkedin.metadata.test.query.StructuredPropertyEvaluator;
import com.linkedin.metadata.test.query.SystemAspectEvaluator;
import com.linkedin.metadata.test.query.virtualFields.VirtualFieldsQueryEvaluator;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestSource;
import com.linkedin.test.TestSourceType;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.core.io.ClassPathResource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Slf4j
public class FormServiceTest {

  private static final String TEST_FORM_PROMPT_TEST_DEFINITION_PATH =
      "./forms/form_prompt_test_definition.json";
  private static final String TEST_GLOSSARY_TERMS_PROMPT_TEST_DEFINITION_PATH =
      "./forms/form_prompt_test_definition_glossary_terms.json";
  private static final String TEST_GLOSSARY_TERMS_ALLOWED_LIST_PROMPT_TEST_DEFINITION_PATH =
      "./forms/form_prompt_test_definition_glossary_terms_allowed_list.json";
  private static final String TEST_FORM_OWNERSHIP_PROMPT_TEST_DEFINITION_PATH =
      "./forms/form_ownership_prompt_test_definition.json";
  private static final String TEST_FORM_OWNERSHIP_WITH_PARAMS_PROMPT_TEST_DEFINITION_PATH =
      "./forms/form_ownership_with_params_prompt_test_definition.json";
  private static final String TEST_FORM_DOMAIN_PROMPT_TEST_DEFINITION_PATH =
      "./forms/form_domain_prompt_test_definition.json";
  private static final String TEST_FORM_DOMAIN_WITH_PARAMS_PROMPT_TEST_DEFINITION_PATH =
      "./forms/form_domain_with_params_prompt_test_definition.json";
  private static final String TEST_FORM_ASSIGNMENT_TEST_DEFINITION_SIMPLE_PATH =
      "./forms/form_assignment_test_definition_simple.json";
  private static final String TEST_FORM_ASSIGNMENT_TEST_DEFINITION_COMPLEX_PATH =
      "./forms/form_assignment_test_definition_complex.json";

  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,Test,PROD)");
  private static final Urn TEST_FORM_URN = UrnUtils.getUrn("urn:li:form:test");
  public static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:jdoe");
  public static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:test");

  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.userContextNoSearchAuthorization(TEST_ACTOR_URN);
  }

  @Test
  private void testBatchAssignFormToEntitiesDoNotExist() throws Exception {
    // Case 1 - non existing form.
    Urn nonExistantForm = UrnUtils.getUrn("urn:li:form:non-existant");
    SystemEntityClient mockClient = mockEntityClient(null, null);
    Mockito.when(mockClient.exists(any(OperationContext.class), eq(nonExistantForm), anyBoolean()))
        .thenReturn(false);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          formService.batchAssignFormToEntities(
              any(OperationContext.class), ImmutableList.of(TEST_ENTITY_URN), nonExistantForm);
        });
  }

  @Test
  private void testBatchAssignFormToEntitiesEmptyForms() throws Exception {

    Forms existingForms = new Forms();
    existingForms.setCompletedForms(new FormAssociationArray());
    existingForms.setIncompleteForms(new FormAssociationArray());
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo formToAdd = new FormInfo();
    formToAdd.setType(FormType.VERIFICATION);
    formToAdd.setDescription("Test description");
    formToAdd.setName("Test name");
    formToAdd.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, formToAdd);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchAssignFormToEntities(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN);

    Forms expectedForms = new Forms();
    expectedForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId("test-id")
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    expectedForms.setCompletedForms(new FormAssociationArray());
    expectedForms.setVerifications(new FormVerificationAssociationArray());

    // Ensure that the forms aspect was ingested for the entity.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN, FORMS_ASPECT_NAME, expectedForms)))),
            anyBoolean());
  }

  @Test
  private void testBatchAssignFormToEntitiesNoExistingForms() throws Exception {

    FormInfo formToAdd = new FormInfo();
    formToAdd.setType(FormType.VERIFICATION);
    formToAdd.setDescription("Test description");
    formToAdd.setName("Test name");
    formToAdd.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(null, formToAdd);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchAssignFormToEntities(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN);

    Forms expectedForms = new Forms();
    expectedForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId("test-id")
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    expectedForms.setCompletedForms(new FormAssociationArray());
    expectedForms.setVerifications(new FormVerificationAssociationArray());

    // Ensure that the forms aspect was ingested for the entity.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN, FORMS_ASPECT_NAME, expectedForms)))),
            anyBoolean());
  }

  @Test
  private void testBatchAssignFormToEntitiesFormAlreadyAppliedCompleted() throws Exception {

    // Form already applied + completed.
    Forms existingForms = new Forms();
    existingForms.setCompletedForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId("test-id")
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    existingForms.setIncompleteForms(new FormAssociationArray());
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo formToAdd = new FormInfo();
    formToAdd.setType(FormType.VERIFICATION);
    formToAdd.setDescription("Test description");
    formToAdd.setName("Test name");
    formToAdd.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, formToAdd);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchAssignFormToEntities(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN);

    // Ensure that no aspect was ingested, because nothing changed.
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(true));
  }

  @Test
  private void testBatchAssignFormToEntitiesFormAlreadyAppliedIncomplete() throws Exception {

    // Form already applied + incomplete.
    Forms existingForms = new Forms();
    existingForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId("test-id")
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    existingForms.setCompletedForms(new FormAssociationArray());
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo formToAdd = new FormInfo();
    formToAdd.setType(FormType.VERIFICATION);
    formToAdd.setDescription("Test description");
    formToAdd.setName("Test name");
    formToAdd.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, formToAdd);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchAssignFormToEntities(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN);

    // Ensure that no aspect was ingested, because nothing changed.
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(true));
  }

  @Test
  private void testBatchUnassignFormForEntitiesFormIncomplete() throws Exception {

    Forms existingForms = new Forms();
    existingForms.setCompletedForms(new FormAssociationArray());
    existingForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId("test-id")
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo formToRemove = new FormInfo();
    formToRemove.setType(FormType.VERIFICATION);
    formToRemove.setDescription("Test description");
    formToRemove.setName("Test name");
    formToRemove.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, formToRemove);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchUnassignFormForEntities(
        mockOperationContext(), ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN);

    Forms expectedForms = new Forms();
    expectedForms.setIncompleteForms(new FormAssociationArray());
    expectedForms.setCompletedForms(new FormAssociationArray());
    expectedForms.setVerifications(new FormVerificationAssociationArray());

    // Ensure that the forms aspect was ingested for the entity.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN, FORMS_ASPECT_NAME, expectedForms)))),
            anyBoolean());
  }

  @Test
  private void testBatchUnassignFormForEntitiesFormComplete() throws Exception {

    Forms existingForms = new Forms();
    existingForms.setIncompleteForms(new FormAssociationArray());
    existingForms.setCompletedForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId("test-id")
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo formToRemove = new FormInfo();
    formToRemove.setType(FormType.VERIFICATION);
    formToRemove.setDescription("Test description");
    formToRemove.setName("Test name");
    formToRemove.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, formToRemove);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchUnassignFormForEntities(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN);

    Forms expectedForms = new Forms();
    expectedForms.setIncompleteForms(new FormAssociationArray());
    expectedForms.setCompletedForms(new FormAssociationArray());
    expectedForms.setVerifications(new FormVerificationAssociationArray());

    // Ensure that the forms aspect was ingested for the entity.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN, FORMS_ASPECT_NAME, expectedForms)))),
            anyBoolean());
  }

  @Test
  private void testBatchUnassignFormForFormNotApplied() throws Exception {

    Forms existingForms = new Forms();
    existingForms.setCompletedForms(new FormAssociationArray());
    existingForms.setIncompleteForms(new FormAssociationArray());
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo formToRemove = new FormInfo();
    formToRemove.setType(FormType.VERIFICATION);
    formToRemove.setDescription("Test description");
    formToRemove.setName("Test name");
    formToRemove.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, formToRemove);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchUnassignFormForEntities(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN);

    // Ensure that the forms aspect was not ingested: no changes required.
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class));
  }

  @Test
  private void testBatchSetFormPromptIncompleteEntitiesDoNotExist() throws Exception {

    // Case 1 - non existing form.
    Urn nonExistantForm = UrnUtils.getUrn("urn:li:form:non-existant");
    SystemEntityClient mockClient = mockEntityClient(null, null);
    Mockito.when(mockClient.exists(any(OperationContext.class), eq(nonExistantForm)))
        .thenReturn(false);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          formService.batchSetFormPromptIncomplete(
              opContext, ImmutableList.of(TEST_ENTITY_URN), nonExistantForm, "test-id");
        });

    // Case 2 - non existant entity.
    Urn nonExistantEntity =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test-2,PROD)");
    Mockito.when(mockClient.exists(any(OperationContext.class), eq(nonExistantEntity)))
        .thenReturn(false);

    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          formService.batchSetFormPromptIncomplete(
              opContext, ImmutableList.of(nonExistantEntity), TEST_FORM_URN, "test-id");
        });
  }

  @Test
  private void testBatchSetFormPromptIncompletePromptAlreadyIncomplete() throws Exception {

    String promptId = "test-id";

    Forms existingForms = new Forms();
    existingForms.setCompletedForms(new FormAssociationArray());
    existingForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId(promptId)
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo form = new FormInfo();
    form.setType(FormType.VERIFICATION);
    form.setDescription("Test description");
    form.setName("Test name");
    form.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId(promptId)
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, form);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchSetFormPromptIncomplete(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN, promptId);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN, FORMS_ASPECT_NAME, existingForms)))),
            Mockito.eq(false));
  }

  @Test
  private void testBatchSetFormPromptIncompleteFormPromptComplete() throws Exception {

    String promptId = "test-id";

    Forms existingForms = new Forms();
    existingForms.setIncompleteForms(new FormAssociationArray());
    existingForms.setCompletedForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setCompletedPrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId(promptId)
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setIncompletePrompts(new FormPromptAssociationArray()))));
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo form = new FormInfo();
    form.setType(FormType.VERIFICATION);
    form.setDescription("Test description");
    form.setName("Test name");
    form.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId(promptId)
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, form);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchSetFormPromptIncomplete(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN, promptId);

    Forms expectedForms = new Forms();
    expectedForms.setCompletedForms(new FormAssociationArray());
    expectedForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId(promptId)
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    expectedForms.setVerifications(new FormVerificationAssociationArray());

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN, FORMS_ASPECT_NAME, expectedForms)))),
            anyBoolean());
  }

  @Test
  private void testBatchSetFormPromptIncompleteFormIsNotAssociated() throws Exception {
    String promptId = "test-id";

    Forms existingForms = new Forms();
    existingForms.setIncompleteForms(new FormAssociationArray());
    existingForms.setCompletedForms(new FormAssociationArray());
    existingForms.setVerifications(new FormVerificationAssociationArray());

    FormInfo form = new FormInfo();
    form.setType(FormType.VERIFICATION);
    form.setDescription("Test description");
    form.setName("Test name");
    form.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId(promptId)
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, form);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchSetFormPromptIncomplete(
        mockOperationContext(), ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN, promptId);

    // No changes applied, since form was not assigned.
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class));
  }

  @Test
  private void testBatchSetFormPromptIncompleteVerificationRemoved() throws Exception {
    String promptId = "test-id";

    Forms existingForms = new Forms();
    existingForms.setIncompleteForms(new FormAssociationArray());
    existingForms.setCompletedForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setCompletedPrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId(promptId)
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setIncompletePrompts(new FormPromptAssociationArray()))));
    // Existing verification.
    existingForms.setVerifications(
        new FormVerificationAssociationArray(
            ImmutableList.of(new FormVerificationAssociation().setForm(TEST_FORM_URN))));

    FormInfo form = new FormInfo();
    form.setType(FormType.VERIFICATION);
    form.setDescription("Test description");
    form.setName("Test name");
    form.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId(promptId)
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, form);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchSetFormPromptIncomplete(
        mockOperationContext(), ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN, promptId);

    Forms expectedForms = new Forms();
    expectedForms.setCompletedForms(new FormAssociationArray());
    expectedForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId(promptId)
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    // Verification removed.
    expectedForms.setVerifications(new FormVerificationAssociationArray());

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN, FORMS_ASPECT_NAME, expectedForms)))),
            anyBoolean());
  }

  @Test
  private void testBatchSetFormPromptIncompleteVerificationNotRemoved() throws Exception {
    String promptId = "test-id";

    Forms existingForms = new Forms();
    existingForms.setIncompleteForms(new FormAssociationArray());
    existingForms.setCompletedForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setCompletedPrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId(promptId)
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setIncompletePrompts(new FormPromptAssociationArray()))));
    // Existing verification.
    existingForms.setVerifications(
        new FormVerificationAssociationArray(
            ImmutableList.of(new FormVerificationAssociation().setForm(TEST_FORM_URN))));

    FormInfo form = new FormInfo();
    form.setType(FormType.VERIFICATION);
    form.setDescription("Test description");
    form.setName("Test name");
    form.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId(promptId)
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(
                        false) // THE PROMPT IS NOT REQUIRED, MEANING THE FORM SHOULD NOT BE MARKED
                    // AS INCOMPLETE.
                    .setTitle("Test title")
                    .setDescription("Test description"))));

    SystemEntityClient mockClient = mockEntityClient(existingForms, form);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.batchSetFormPromptIncomplete(
        opContext, ImmutableList.of(TEST_ENTITY_URN), TEST_FORM_URN, promptId);

    Forms expectedForms = new Forms();
    expectedForms.setIncompleteForms(new FormAssociationArray());
    expectedForms.setCompletedForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId(promptId)
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    // Form still completed, verification unchanged.
    expectedForms.setVerifications(
        new FormVerificationAssociationArray(
            ImmutableList.of(new FormVerificationAssociation().setForm(TEST_FORM_URN))));

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN, FORMS_ASPECT_NAME, expectedForms)))),
            anyBoolean());
  }

  @Test
  private void testUpsertFormPromptCompletionAutomation() throws Exception {
    // Verify that a test of the expected format is created.
    String promptId = "test-id";
    Urn testPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test.id");
    FormPrompt prompt =
        new FormPrompt()
            .setId(promptId)
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setTitle("Test Title")
            .setDescription("Test Description")
            .setRequired(true)
            .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(testPropertyUrn));

    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    formService.upsertFormPromptCompletionAutomation(
        opContext, TEST_FORM_URN, Collections.singletonList(prompt));
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(new ClassPathResource(TEST_FORM_PROMPT_TEST_DEFINITION_PATH).getFile());
    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormPrompt(TEST_FORM_URN, prompt);
    TestInfo expectedTestInfo =
        createExpectedTestInfo(testDefinition, promptId, TestSourceType.FORM_PROMPT);
    // Verify that the correct test was ingested.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
  }

  @Test
  private void testUpsertOwnershipFormPromptCompletionAutomation() throws Exception {
    // Verify that a test of the expected format is created for ownership with no extra params
    String promptId = "test-id";
    FormPrompt prompt =
        new FormPrompt()
            .setId(promptId)
            .setType(FormPromptType.OWNERSHIP)
            .setTitle("Test Title")
            .setDescription("Test Description")
            .setRequired(true)
            .setOwnershipParams(new OwnershipParams().setCardinality(PromptCardinality.MULTIPLE));

    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    formService.upsertFormPromptCompletionAutomation(
        opContext, TEST_FORM_URN, Collections.singletonList(prompt));
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(
                new ClassPathResource(TEST_FORM_OWNERSHIP_PROMPT_TEST_DEFINITION_PATH).getFile());
    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormPrompt(TEST_FORM_URN, prompt);
    TestInfo expectedTestInfo =
        createExpectedTestInfo(testDefinition, promptId, TestSourceType.FORM_PROMPT);
    // Verify that the correct test was ingested.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
  }

  @Test
  private void testUpsertOwnershipWithParamsFormPromptCompletionAutomation() throws Exception {
    // Verify that a test of the expected format is created for ownership with no extra params
    String promptId = "test-id";
    OwnershipParams ownershipParams = new OwnershipParams();
    UrnArray allowedOwners =
        new UrnArray(
            ImmutableList.of(
                UrnUtils.getUrn("urn:li:corpuser:admin"),
                UrnUtils.getUrn("urn:li:corpGroup:jdoe")));
    ownershipParams.setAllowedOwners(allowedOwners);
    UrnArray allowedOwnershipTypes =
        new UrnArray(
            ImmutableList.of(
                UrnUtils.getUrn("urn:li:ownershipType:test1"),
                UrnUtils.getUrn("urn:li:ownershipType:test2")));
    ownershipParams.setAllowedOwnershipTypes(allowedOwnershipTypes);
    FormPrompt prompt =
        new FormPrompt()
            .setId(promptId)
            .setType(FormPromptType.OWNERSHIP)
            .setTitle("Test Title")
            .setDescription("Test Description")
            .setRequired(true)
            .setOwnershipParams(ownershipParams);

    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    formService.upsertFormPromptCompletionAutomation(
        opContext, TEST_FORM_URN, Collections.singletonList(prompt));
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(
                new ClassPathResource(TEST_FORM_OWNERSHIP_WITH_PARAMS_PROMPT_TEST_DEFINITION_PATH)
                    .getFile());
    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormPrompt(TEST_FORM_URN, prompt);
    TestInfo expectedTestInfo =
        createExpectedTestInfo(testDefinition, promptId, TestSourceType.FORM_PROMPT);
    // Verify that the correct test was ingested.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
  }

  @Test
  private void testUpsertGlossaryTermsPromptCompletionAutomation() throws Exception {
    // Verify that a test of the expected format is created.
    // This test should assert that the asset has any glossary term on it
    String promptId = "test-id";
    FormPrompt prompt =
        new FormPrompt()
            .setId(promptId)
            .setType(FormPromptType.GLOSSARY_TERMS)
            .setTitle("Test Title")
            .setDescription("Test Description")
            .setRequired(true)
            .setGlossaryTermsParams(
                new GlossaryTermsParams().setCardinality(PromptCardinality.MULTIPLE));

    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    formService.upsertFormPromptCompletionAutomation(
        opContext, TEST_FORM_URN, Collections.singletonList(prompt));
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(
                new ClassPathResource(TEST_GLOSSARY_TERMS_PROMPT_TEST_DEFINITION_PATH).getFile());
    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormPrompt(TEST_FORM_URN, prompt);
    TestInfo expectedTestInfo =
        createExpectedTestInfo(testDefinition, promptId, TestSourceType.FORM_PROMPT);
    // Verify that the correct test was ingested.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
  }

  @Test
  private void testUpsertGlossaryTermsAllowedListPromptCompletionAutomation() throws Exception {
    // Verify that a test of the expected format is created.
    // This test should assert that the asset has two different glossary terms on it
    // one explicit and one from a term group set
    String promptId = "test-id";
    FormPrompt prompt =
        new FormPrompt()
            .setId(promptId)
            .setType(FormPromptType.GLOSSARY_TERMS)
            .setTitle("Test Title")
            .setDescription("Test Description")
            .setRequired(true)
            .setGlossaryTermsParams(
                new GlossaryTermsParams()
                    .setCardinality(PromptCardinality.MULTIPLE)
                    .setAllowedTermGroups(
                        new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:glossaryNode:test"))))
                    .setAllowedTerms(
                        new UrnArray(
                            ImmutableList.of(UrnUtils.getUrn("urn:li:glossaryTerm:test1")))));

    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    formService.upsertFormPromptCompletionAutomation(
        opContext, TEST_FORM_URN, Collections.singletonList(prompt));
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(
                new ClassPathResource(TEST_GLOSSARY_TERMS_ALLOWED_LIST_PROMPT_TEST_DEFINITION_PATH)
                    .getFile());
    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormPrompt(TEST_FORM_URN, prompt);
    TestInfo expectedTestInfo =
        createExpectedTestInfo(testDefinition, promptId, TestSourceType.FORM_PROMPT);
    // Verify that the correct test was ingested.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
  }

  @Test
  private void testUpsertDomainPromptCompletionAutomation() throws Exception {
    // Verify that a test of the expected format is created.
    // This test should assert that the asset has any domain on it
    String promptId = "test-id";
    FormPrompt prompt =
        new FormPrompt()
            .setId(promptId)
            .setType(FormPromptType.DOMAIN)
            .setTitle("Test Title")
            .setDescription("Test Description")
            .setRequired(true);

    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    formService.upsertFormPromptCompletionAutomation(
        opContext, TEST_FORM_URN, Collections.singletonList(prompt));
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(
                new ClassPathResource(TEST_FORM_DOMAIN_PROMPT_TEST_DEFINITION_PATH).getFile());
    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormPrompt(TEST_FORM_URN, prompt);
    TestInfo expectedTestInfo =
        createExpectedTestInfo(testDefinition, promptId, TestSourceType.FORM_PROMPT);
    // Verify that the correct test was ingested.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
  }

  @Test
  private void testUpsertDomainAllowedListPromptCompletionAutomation() throws Exception {
    // Verify that a test of the expected format is created.
    // This test should assert that the asset has one of two domains on it
    String promptId = "test-id";
    FormPrompt prompt =
        new FormPrompt()
            .setId(promptId)
            .setType(FormPromptType.DOMAIN)
            .setTitle("Test Title")
            .setDescription("Test Description")
            .setRequired(true)
            .setDomainParams(
                new DomainParams()
                    .setAllowedDomains(
                        new UrnArray(
                            ImmutableList.of(
                                UrnUtils.getUrn("urn:li:domain:test1"),
                                UrnUtils.getUrn("urn:li:domain:test2")))));

    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    formService.upsertFormPromptCompletionAutomation(
        opContext, TEST_FORM_URN, Collections.singletonList(prompt));
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(
                new ClassPathResource(TEST_FORM_DOMAIN_WITH_PARAMS_PROMPT_TEST_DEFINITION_PATH)
                    .getFile());
    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormPrompt(TEST_FORM_URN, prompt);
    TestInfo expectedTestInfo =
        createExpectedTestInfo(testDefinition, promptId, TestSourceType.FORM_PROMPT);
    // Verify that the correct test was ingested.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
  }

  private TestInfo createExpectedTestInfo(
      JsonNode testDefinition, String promptId, TestSourceType sourceType) {
    return new TestInfo()
        .setName(String.format("Form Prompts Test - %s, Prompt Id - %s", TEST_FORM_URN, promptId))
        .setDescription(
            String.format(
                "This test was auto-generated to implement form assignment for form with urn %s",
                TEST_FORM_URN))
        .setCategory("Forms")
        .setSource(new TestSource().setType(sourceType).setSourceEntity(TEST_FORM_URN))
        .setDefinition(
            new TestDefinition()
                .setType(TestDefinitionType.JSON)
                .setJson(testDefinition.toString()));
  }

  @Test
  private void testUpsertFormAssignmentAutomationSimple() throws Exception {
    // Verify that a test of the expected format is created.
    DynamicFormAssignment formAssignment =
        new DynamicFormAssignment()
            .setFilter(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            ImmutableList.of(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(
                                            ImmutableList.of(
                                                new Criterion()
                                                    .setField("platform")
                                                    .setCondition(Condition.EQUAL)
                                                    .setValue("urn:li:dataPlatform:hive")
                                                    .setValues(
                                                        new StringArray(
                                                            ImmutableList.of(
                                                                "urn:li:dataPlatform:hive")))
                                                    .setNegated(false))))))));
    FormInfo formToAdd = new FormInfo();
    formToAdd.setType(FormType.VERIFICATION);
    formToAdd.setDescription("Test description");
    formToAdd.setName("Test name");
    formToAdd.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));
    SystemEntityClient mockClient = mockEntityClient(null, formToAdd);
    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(TEST_ENTITY_URN);
    searchEntities.add(searchEntity);
    Mockito.when(
            mockClient.scrollAcrossEntities(
                any(OperationContext.class),
                anyList(),
                anyString(),
                nullable(Filter.class),
                nullable(String.class),
                nullable(String.class),
                anyInt(),
                eq(null),
                eq(
                    "{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"AND\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"platform\",\"queryParts\":[\"platform\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:dataPlatform:hive\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}")))
        .thenReturn(new ScrollResult().setNumEntities(1).setEntities(searchEntities));
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    Thread assignThread =
        formService.upsertFormAssignmentAutomation(opContext, TEST_FORM_URN, formAssignment);
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(
                new ClassPathResource(TEST_FORM_ASSIGNMENT_TEST_DEFINITION_SIMPLE_PATH).getFile());
    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormAssignment(TEST_FORM_URN);
    TestInfo expectedTestInfo =
        new TestInfo()
            .setName(String.format("Form Assignment Test - %s", TEST_FORM_URN))
            .setDescription(
                String.format(
                    "This test was auto-generated to implement form assignment for form with urn %s",
                    TEST_FORM_URN))
            .setCategory("Forms")
            .setSource(
                new TestSource().setType(TestSourceType.FORMS).setSourceEntity(TEST_FORM_URN))
            .setDefinition(
                new TestDefinition()
                    .setType(TestDefinitionType.JSON)
                    .setJson(testDefinition.toString()));
    assignThread.join();
    // Verify that the correct test was ingested.
    InOrder inOrder = Mockito.inOrder(mockClient);
    inOrder
        .verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
    inOrder
        .verify(mockClient, Mockito.times(1))
        .batchIngestProposals(any(OperationContext.class), notNull(), eq(true));
  }

  @Test
  private void testUpsertFormAssignmentAutomationComplex() throws Exception {
    DynamicFormAssignment formAssignment =
        new DynamicFormAssignment()
            .setFilter(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            ImmutableList.of(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(
                                            ImmutableList.of(
                                                buildCriterion(
                                                    "platform", "urn:li:dataPlatform:hive"),
                                                buildCriterion(
                                                    "container", "urn:li:container:test"),
                                                buildCriterion("_entityType", "dataset"),
                                                buildCriterion("domains", "urn:li:domain:test")))),
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(
                                            ImmutableList.of(
                                                buildCriterion(
                                                    "platform", "urn:li:dataPlatform:snowflake"),
                                                buildCriterion(
                                                    "container", "urn:li:container:test-2"),
                                                buildCriterion("_entityType", "dashboard"),
                                                buildCriterion(
                                                    "domains", "urn:li:domain:test-2"))))))));
    FormInfo formToAdd = new FormInfo();
    formToAdd.setType(FormType.VERIFICATION);
    formToAdd.setDescription("Test description");
    formToAdd.setName("Test name");
    formToAdd.setPrompts(
        new FormPromptArray(
            ImmutableSet.of(
                new FormPrompt()
                    .setId("test-id")
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(true)
                    .setTitle("Test title")
                    .setDescription("Test description"))));
    SystemEntityClient mockClient = mockEntityClient(null, formToAdd);
    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(TEST_ENTITY_URN);
    searchEntities.add(searchEntity);
    Mockito.when(
            mockClient.scrollAcrossEntities(
                any(OperationContext.class),
                anyList(),
                anyString(),
                nullable(Filter.class),
                nullable(String.class),
                nullable(String.class),
                anyInt(),
                eq(null),
                eq(
                    "{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"AND\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"platform\",\"queryParts\":[\"platform\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:dataPlatform:hive\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"container\",\"queryParts\":[\"container\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:container:test\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":2,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"_entityType\",\"queryParts\":[\"_entityType\"]}}},{\"index\":1,\"expression\":{\"values\":[\"dataset\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":3,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"domains\",\"queryParts\":[\"domains\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:domain:test\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"AND\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"platform\",\"queryParts\":[\"platform\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:dataPlatform:snowflake\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"container\",\"queryParts\":[\"container\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:container:test-2\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":2,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"_entityType\",\"queryParts\":[\"_entityType\"]}}},{\"index\":1,\"expression\":{\"values\":[\"dashboard\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":3,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"domains\",\"queryParts\":[\"domains\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:domain:test-2\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}")))
        .thenReturn(new ScrollResult().setNumEntities(1).setEntities(searchEntities));
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    Thread assignThread =
        formService.upsertFormAssignmentAutomation(opContext, TEST_FORM_URN, formAssignment);
    JsonNode testDefinition =
        new ObjectMapper()
            .readTree(
                new ClassPathResource(TEST_FORM_ASSIGNMENT_TEST_DEFINITION_COMPLEX_PATH).getFile());

    // Validate test definition
    final EntityService<?> entityService = mock(EntityService.class);
    final QueryVersionedAspectEvaluator queryVersionedAspectEvaluator =
        new QueryVersionedAspectEvaluator(new TestEntityRegistry(), entityService);
    final EntityUrnTypeEvaluator urnTypeEvaluator = new EntityUrnTypeEvaluator();
    final SystemAspectEvaluator systemAspectEvaluator = new SystemAspectEvaluator(entityService);
    final StructuredPropertyEvaluator structuredPropertyEvaluator =
        new StructuredPropertyEvaluator(entityService);
    final VirtualFieldsQueryEvaluator virtualFieldsQueryEvaluator =
        new VirtualFieldsQueryEvaluator();
    QueryEngine queryEngine =
        new QueryEngine(
            ImmutableList.of(
                urnTypeEvaluator,
                queryVersionedAspectEvaluator,
                systemAspectEvaluator,
                structuredPropertyEvaluator,
                virtualFieldsQueryEvaluator));
    TestDefinitionParser testDefinitionParser =
        new TestDefinitionParser(PredicateEvaluator.getInstance());
    TestEngine testEngine =
        new TestEngine(
            opContext,
            true,
            Mockito.mock(EntityServiceImpl.class),
            Mockito.mock(EntitySearchService.class),
            Mockito.mock(TimeseriesAspectService.class),
            Mockito.mock(TestFetcher.class),
            testDefinitionParser,
            queryEngine,
            PredicateEvaluator.getInstance(),
            Mockito.mock(ActionApplier.class),
            100000,
            1000000,
            false,
            new TestsHookExecutionLimitConfiguration());
    ValidationResult validationResult = testEngine.validateJson(testDefinition.toString());
    Assert.assertTrue(
        "Expected valid test JSON: " + validationResult.getMessages(), validationResult.isValid());

    Urn expectedTestUrn = FormTestBuilder.createTestUrnForFormAssignment(TEST_FORM_URN);
    TestInfo expectedTestInfo =
        new TestInfo()
            .setName(String.format("Form Assignment Test - %s", TEST_FORM_URN))
            .setDescription(
                String.format(
                    "This test was auto-generated to implement form assignment for form with urn %s",
                    TEST_FORM_URN))
            .setCategory("Forms")
            .setSource(
                new TestSource().setType(TestSourceType.FORMS).setSourceEntity(TEST_FORM_URN))
            .setDefinition(
                new TestDefinition()
                    .setType(TestDefinitionType.JSON)
                    .setJson(testDefinition.toString()));
    assignThread.join();
    // Verify that the correct test was ingested.
    InOrder inOrder = Mockito.inOrder(mockClient);
    inOrder
        .verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new FormTestArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            expectedTestUrn, TEST_INFO_ASPECT_NAME, expectedTestInfo)))),
            eq(false));
    inOrder
        .verify(mockClient, Mockito.times(1))
        .batchIngestProposals(any(OperationContext.class), notNull(), eq(true));
  }

  @Test
  private void testRemoveFormAssignmentAutomationSimple() throws Exception {
    // Verify that a test of the expected format is created.
    DynamicFormAssignment formAssignment =
        new DynamicFormAssignment()
            .setFilter(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            ImmutableList.of(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(
                                            ImmutableList.of(
                                                new Criterion()
                                                    .setField("platform")
                                                    .setCondition(Condition.EQUAL)
                                                    .setValue("urn:li:dataPlatform:hive")
                                                    .setValues(
                                                        new StringArray(
                                                            ImmutableList.of(
                                                                "urn:li:dataPlatform:hive")))
                                                    .setNegated(false))))))));
    Forms expectedForms = new Forms();
    expectedForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId("test-id")
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    expectedForms.setCompletedForms(new FormAssociationArray());
    expectedForms.setVerifications(new FormVerificationAssociationArray());
    SystemEntityClient mockClient = mockEntityClient(expectedForms, null);
    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(TEST_ENTITY_URN);
    searchEntities.add(searchEntity);
    Mockito.when(
            mockClient.scrollAcrossEntities(
                any(OperationContext.class),
                anyList(),
                anyString(),
                nullable(Filter.class),
                nullable(String.class),
                nullable(String.class),
                anyInt(),
                eq(null),
                eq(
                    "{\"operatorType\":\"AND\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"NOT\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"AND\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"platform\",\"queryParts\":[\"platform\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:dataPlatform:hive\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"incompleteForms\",\"queryParts\":[\"incompleteForms\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:form:test\"]}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"completedForms\",\"queryParts\":[\"completedForms\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:form:test\"]}}],\"nameToOperand\":{}}}},{\"index\":2,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"verifiedForms\",\"queryParts\":[\"verifiedForms\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:form:test\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}")))
        .thenReturn(new ScrollResult().setNumEntities(1).setEntities(searchEntities));
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    Thread unassignThread =
        formService.removeFormAssignmentAutomation(opContext, TEST_FORM_URN, formAssignment);
    unassignThread.join();
    // Verify that the correct test was ingested.
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(any(OperationContext.class), notNull(), eq(true));
  }

  @Test
  private void testRemoveFormAssignmentAutomationComplex() throws Exception {
    DynamicFormAssignment formAssignment =
        new DynamicFormAssignment()
            .setFilter(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            ImmutableList.of(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(
                                            ImmutableList.of(
                                                buildCriterion(
                                                    "platform", "urn:li:dataPlatform:hive"),
                                                buildCriterion(
                                                    "container", "urn:li:container:test"),
                                                buildCriterion("_entityType", "dataset"),
                                                buildCriterion("domains", "urn:li:domain:test")))),
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(
                                            ImmutableList.of(
                                                buildCriterion(
                                                    "platform", "urn:li:dataPlatform:snowflake"),
                                                buildCriterion(
                                                    "container", "urn:li:container:test-2"),
                                                buildCriterion("_entityType", "dashboard"),
                                                buildCriterion(
                                                    "domains", "urn:li:domain:test-2"))))))));
    Forms expectedForms = new Forms();
    expectedForms.setIncompleteForms(
        new FormAssociationArray(
            ImmutableList.of(
                new FormAssociation()
                    .setUrn(TEST_FORM_URN)
                    .setIncompletePrompts(
                        new FormPromptAssociationArray(
                            ImmutableList.of(
                                new FormPromptAssociation()
                                    .setId("test-id")
                                    .setLastModified(
                                        new AuditStamp()
                                            .setTime(0L)
                                            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))))))
                    .setCompletedPrompts(new FormPromptAssociationArray()))));
    expectedForms.setCompletedForms(new FormAssociationArray());
    expectedForms.setVerifications(new FormVerificationAssociationArray());
    SystemEntityClient mockClient = mockEntityClient(expectedForms, null);
    SearchEntityArray searchEntities = new SearchEntityArray();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(TEST_ENTITY_URN);
    searchEntities.add(searchEntity);
    Mockito.when(
            mockClient.scrollAcrossEntities(
                any(OperationContext.class),
                anyList(),
                anyString(),
                nullable(Filter.class),
                nullable(String.class),
                nullable(String.class),
                anyInt(),
                eq(null),
                eq(
                    "{\"operatorType\":\"AND\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"NOT\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"AND\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"platform\",\"queryParts\":[\"platform\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:dataPlatform:hive\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"container\",\"queryParts\":[\"container\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:container:test\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":2,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"_entityType\",\"queryParts\":[\"_entityType\"]}}},{\"index\":1,\"expression\":{\"values\":[\"dataset\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":3,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"domains\",\"queryParts\":[\"domains\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:domain:test\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"AND\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"platform\",\"queryParts\":[\"platform\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:dataPlatform:snowflake\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"container\",\"queryParts\":[\"container\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:container:test-2\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":2,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"_entityType\",\"queryParts\":[\"_entityType\"]}}},{\"index\":1,\"expression\":{\"values\":[\"dashboard\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":3,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"domains\",\"queryParts\":[\"domains\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:domain:test-2\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"OR\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"incompleteForms\",\"queryParts\":[\"incompleteForms\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:form:test\"]}}],\"nameToOperand\":{}}}},{\"index\":1,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"completedForms\",\"queryParts\":[\"completedForms\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:form:test\"]}}],\"nameToOperand\":{}}}},{\"index\":2,\"expression\":{\"operatorType\":\"ANY_EQUALS\",\"operands\":{\"operands\":[{\"index\":0,\"expression\":{\"query\":{\"query\":\"verifiedForms\",\"queryParts\":[\"verifiedForms\"]}}},{\"index\":1,\"expression\":{\"values\":[\"urn:li:form:test\"]}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}}],\"nameToOperand\":{}}}")))
        .thenReturn(new ScrollResult().setNumEntities(1).setEntities(searchEntities));
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());
    Thread unassignThread =
        formService.removeFormAssignmentAutomation(opContext, TEST_FORM_URN, formAssignment);
    unassignThread.join();
    // Verify runner performs removal
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(any(OperationContext.class), notNull(), eq(true));
  }

  @Test
  private void testRemoveAllFormAutomations() throws Exception {
    Urn metadataTestUrn1 = UrnUtils.getUrn("urn:li:test:form-test-1");
    Urn metadataTestUrn2 = UrnUtils.getUrn("urn:li:test:form-test-2");
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.search(
                any(),
                eq(TEST_ENTITY_NAME),
                eq("*"),
                eq(ImmutableMap.of("sourceUrn", TEST_FORM_URN.toString())),
                eq(0),
                Mockito.anyInt()))
        .thenReturn(
            new SearchResult()
                .setNumEntities(2)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(
                            new SearchEntity().setEntity(metadataTestUrn1),
                            new SearchEntity().setEntity(metadataTestUrn2)))));

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.removeAllFormAutomations(opContext, TEST_FORM_URN);

    // Verify both tests are deleted.
    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), eq(metadataTestUrn1));

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), eq(metadataTestUrn2));
  }

  @Test
  private void testRemoveFormPromptCompletionAutomation() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    FormPrompt prompt =
        new FormPrompt()
            .setId("test-id")
            .setRequired(true)
            .setTitle("Test Title")
            .setDescription("Test Description");

    Urn metadataTestUrn = FormTestBuilder.createTestUrnForFormPrompt(TEST_FORM_URN, prompt);

    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    formService.removeFormPromptCompletionAutomation(mockOperationContext(), TEST_FORM_URN, prompt);

    // Verify the test is deleted.
    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), eq(metadataTestUrn));
  }

  @Test
  private void testIsFormAssignedToUsersWithOwners() throws Exception {
    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    FormActorAssignment formActors = new FormActorAssignment();
    formActors.setOwners(true);
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_FORM_URN.getEntityType()),
                eq(TEST_FORM_URN),
                eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // ensure this user is an explicit owner
    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(TEST_ACTOR_URN);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_ENTITY_URN.getEntityType()),
                eq(TEST_ENTITY_URN),
                eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertTrue(
        formService.isFormAssignedToUser(
            mockOperationContext(),
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            new ArrayList<>()));
  }

  @Test
  private void testIsFormAssignedToUserExplicitly() throws Exception {
    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    FormActorAssignment formActors = new FormActorAssignment();
    formActors.setOwners(false);
    // explicitly set this actor as assigned
    formActors.setUsers(new UrnArray(ImmutableList.of(TEST_ACTOR_URN)));
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_FORM_URN.getEntityType()),
                eq(TEST_FORM_URN),
                eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // this user is not an owner
    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(null);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_ENTITY_URN.getEntityType()),
                eq(TEST_ENTITY_URN),
                eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertTrue(
        formService.isFormAssignedToUser(
            mockOperationContext(),
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            new ArrayList<>()));
  }

  @Test
  private void testIsFormAssignedToUserGroupExplicitly() throws Exception {
    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    FormActorAssignment formActors = new FormActorAssignment();
    formActors.setOwners(false);
    // explicitly set this actor as assigned
    formActors.setGroups(new UrnArray(ImmutableList.of(TEST_GROUP_URN)));
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_FORM_URN.getEntityType()),
                eq(TEST_FORM_URN),
                eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // no owners on this entity
    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(null);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_ENTITY_URN.getEntityType()),
                eq(TEST_ENTITY_URN),
                eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertTrue(
        formService.isFormAssignedToUser(
            mockOperationContext(),
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            ImmutableList.of(TEST_GROUP_URN)));
  }

  @Test
  private void testIsFormIsNotAssignedToUser() throws Exception {
    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    FormActorAssignment formActors = new FormActorAssignment();
    // owners is set to false, no actors or groups assigned means this test will return false
    formActors.setOwners(false);
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_FORM_URN.getEntityType()),
                eq(TEST_FORM_URN),
                eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(null);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_ENTITY_URN.getEntityType()),
                eq(TEST_ENTITY_URN),
                eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertFalse(
        formService.isFormAssignedToUser(
            opContext,
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            ImmutableList.of(TEST_GROUP_URN)));
  }

  @Test
  private void testIsFormAssignedToGroupOwner() throws Exception {
    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    FormActorAssignment formActors = new FormActorAssignment();
    formActors.setOwners(true);
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_FORM_URN.getEntityType()),
                eq(TEST_FORM_URN),
                eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // group that the user is in is assigned
    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(TEST_GROUP_URN);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_ENTITY_URN.getEntityType()),
                eq(TEST_ENTITY_URN),
                eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertTrue(
        formService.isFormAssignedToUser(
            opContext,
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            ImmutableList.of(TEST_GROUP_URN)));
  }

  @Test
  private void testVerifyFormForEntity() throws Exception {
    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    // form type VERIFICATION
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, new FormActorAssignment());
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_FORM_URN.getEntityType()),
                eq(TEST_FORM_URN),
                eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    final FormAssociation formAssociation = new FormAssociation();
    formAssociation.setUrn(TEST_FORM_URN);
    final List<FormAssociation> completedForms = ImmutableList.of(formAssociation);
    Map<String, EnvelopedAspect> formsAspectMap = createFormsAspectMap(completedForms);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_ENTITY_URN.getEntityType()),
                eq(TEST_ENTITY_URN),
                eq(ImmutableSet.of(FORMS_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formsAspectMap)));

    Assert.assertTrue(
        formService.verifyFormForEntity(opContext, TEST_FORM_URN, TEST_ENTITY_URN, null));

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    Collections.singletonList(
                        AspectUtils.buildMetadataChangeProposal(
                            TEST_ENTITY_URN,
                            FORMS_ASPECT_NAME,
                            formsAspectMap.get(FORMS_ASPECT_NAME))))),
            eq(false));
  }

  @Test
  private void testVerifyFormForEntityNonVerificationForm() throws Exception {
    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    // form type COMPLETION
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.COMPLETION, new FormActorAssignment());
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_FORM_URN.getEntityType()),
                eq(TEST_FORM_URN),
                eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          formService.verifyFormForEntity(
              any(OperationContext.class), TEST_FORM_URN, TEST_ENTITY_URN, null);
        });

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class));
  }

  @Test
  private void testVerifyFormForEntityIncompleteForm() throws Exception {
    SystemEntityClient mockClient = mockEntityClient(null, null);
    FormService formService =
        new FormService(mockClient, Mockito.mock(OpenApiClient.class), new ObjectMapper());

    // form type VERIFICATION
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, new FormActorAssignment());
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_FORM_URN.getEntityType()),
                eq(TEST_FORM_URN),
                eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // no completed forms
    Map<String, EnvelopedAspect> formsAspectMap = createFormsAspectMap(new ArrayList<>());
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(TEST_ENTITY_URN.getEntityType()),
                eq(TEST_ENTITY_URN),
                eq(ImmutableSet.of(FORMS_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formsAspectMap)));

    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          formService.verifyFormForEntity(
              any(OperationContext.class), TEST_FORM_URN, TEST_ENTITY_URN, null);
        });

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class));
  }

  private SystemEntityClient mockEntityClient(Forms existingForms, FormInfo form) throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(mockClient.exists(any(OperationContext.class), eq(TEST_ENTITY_URN)))
        .thenReturn(true);

    Mockito.when(mockClient.exists(any(OperationContext.class), eq(TEST_FORM_URN)))
        .thenReturn(true);

    Mockito.when(
            mockClient.scrollAcrossEntities(
                any(OperationContext.class),
                anyList(),
                anyString(),
                nullable(Filter.class),
                nullable(String.class),
                nullable(String.class),
                anyInt(),
                eq(null),
                nullable(String.class)))
        .thenReturn(new ScrollResult().setNumEntities(0));

    EntityResponse entityResponse = null;
    if (existingForms != null) {
      entityResponse =
          new EntityResponse()
              .setUrn(TEST_ENTITY_URN)
              .setEntityName(DATASET_ENTITY_NAME)
              .setAspects(
                  new EnvelopedAspectMap(
                      ImmutableMap.of(
                          FORMS_ASPECT_NAME,
                          new EnvelopedAspect().setValue(new Aspect(existingForms.data())))));
    } else {
      entityResponse =
          new EntityResponse()
              .setUrn(TEST_ENTITY_URN)
              .setEntityName(DATASET_ENTITY_NAME)
              .setAspects(new EnvelopedAspectMap());
    }

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(DATASET_ENTITY_NAME),
                eq(TEST_ENTITY_URN),
                eq(ImmutableSet.of(FORMS_ASPECT_NAME))))
        .thenReturn(entityResponse);

    if (form != null) {
      Mockito.when(
              mockClient.getV2(
                  any(OperationContext.class),
                  eq(FORM_ENTITY_NAME),
                  eq(TEST_FORM_URN),
                  eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
          .thenReturn(
              new EntityResponse()
                  .setUrn(TEST_FORM_URN)
                  .setEntityName(FORM_ENTITY_NAME)
                  .setAspects(
                      new EnvelopedAspectMap(
                          ImmutableMap.of(
                              FORM_INFO_ASPECT_NAME,
                              new EnvelopedAspect().setValue(new Aspect(form.data()))))));
    }

    return mockClient;
  }

  private Criterion buildCriterion(String field, String value) {
    return new Criterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValue(value)
        .setValues(new StringArray(ImmutableList.of(value)))
        .setNegated(false);
  }

  private OperationContext mockOperationContext() {
    return TestOperationContexts.userContextNoSearchAuthorization(
        Authorizer.EMPTY, mockSystemAuthentication());
  }

  private Authentication mockSystemAuthentication() {
    return mockSystemAuthentication(SYSTEM_ACTOR);
  }

  private Authentication mockSystemAuthentication(String actorUrnStr) {
    Authentication auth = mock(Authentication.class);
    Actor actor = mock(Actor.class);
    Mockito.when(actor.toUrnStr()).thenReturn(actorUrnStr);
    Mockito.when(auth.getActor()).thenReturn(actor);
    return auth;
  }

  private Map<String, EnvelopedAspect> createOwnershipAspectMap(@Nullable final Urn actorUrn) {
    Ownership ownershipAspect = new Ownership();
    OwnerArray owners = new OwnerArray();
    if (actorUrn != null) {
      Owner owner = new Owner();
      owner.setOwner(TEST_ACTOR_URN);
      owners.add(owner);
    }
    ownershipAspect.setOwners(owners);
    Map<String, EnvelopedAspect> ownershipAspectMap = new HashMap<>();
    ownershipAspectMap.put(
        OWNERSHIP_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(ownershipAspect.data())));

    return ownershipAspectMap;
  }

  private Map<String, EnvelopedAspect> createFormInfoAspectMap(
      @Nonnull final FormType formType, @Nonnull FormActorAssignment actors) {
    FormInfo formInfo = new FormInfo();
    formInfo.setType(formType);
    formInfo.setActors(actors);
    Map<String, EnvelopedAspect> formInfoAspectMap = new HashMap<>();
    formInfoAspectMap.put(
        FORM_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(formInfo.data())));

    return formInfoAspectMap;
  }

  private Map<String, EnvelopedAspect> createFormsAspectMap(
      @Nonnull final List<FormAssociation> completedForms) {
    Forms forms = new Forms();
    forms.setCompletedForms(new FormAssociationArray(completedForms));
    forms.setVerifications(new FormVerificationAssociationArray());
    Map<String, EnvelopedAspect> formsAspectMap = new HashMap<>();
    formsAspectMap.put(FORMS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(forms.data())));

    return formsAspectMap;
  }
}
