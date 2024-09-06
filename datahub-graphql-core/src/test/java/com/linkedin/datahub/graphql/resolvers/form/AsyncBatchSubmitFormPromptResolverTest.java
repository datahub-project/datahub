package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AsyncBatchSubmitFormPromptInput;
import com.linkedin.datahub.graphql.generated.AsyncBatchSubmitFormPromptResponse;
import com.linkedin.datahub.graphql.generated.DomainInputParams;
import com.linkedin.datahub.graphql.generated.FormFilter;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.GlossaryTermsInputParams;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyInputParams;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.service.FormService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.MetadataTestClient;
import graphql.com.google.common.collect.ImmutableList;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AsyncBatchSubmitFormPromptResolverTest {
  private static final String TEST_USER_URN = "urn:li:corpuser:admin";
  private static final String TEST_FORM_URN = "urn:li:form:1";
  private static final String TEST_TASK_URN = "urn:li:test:1";
  private static final String TEST_PROPERTY_URN = "urn:li:structuredProperty:1";
  private static final String TEST_PROMPT_ID = "123";

  private static final StructuredPropertyInputParams PROPERTIES_INPUT =
      new StructuredPropertyInputParams(
          TEST_PROPERTY_URN, ImmutableList.of(new PropertyValueInput("test value", null)));
  private static final SubmitFormPromptInput PROMPT_INPUT =
      new SubmitFormPromptInput(
          TEST_PROMPT_ID,
          TEST_FORM_URN,
          FormPromptType.STRUCTURED_PROPERTY,
          null,
          null,
          PROPERTIES_INPUT,
          null,
          null,
          null,
          null);
  private static final AsyncBatchSubmitFormPromptInput TEST_INPUT =
      new AsyncBatchSubmitFormPromptInput(
          new ArrayList<>(),
          null,
          new ArrayList<>(),
          new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
          PROMPT_INPUT,
          null);

  @Test
  public void testGetSuccess() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    AsyncBatchSubmitFormPromptResponse response = resolver.get(mockEnv).get();

    assertEquals(response.getTaskUrn(), TEST_TASK_URN);

    // Validate that we called submit to create this test
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetInvalidInputStructuredProp() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);

    // null structured properties params with structured prop input
    final SubmitFormPromptInput promptInput =
        new SubmitFormPromptInput(
            TEST_PROMPT_ID,
            TEST_FORM_URN,
            FormPromptType.STRUCTURED_PROPERTY,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    final AsyncBatchSubmitFormPromptInput testInput =
        new AsyncBatchSubmitFormPromptInput(
            new ArrayList<>(),
            null,
            new ArrayList<>(),
            new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
            promptInput,
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never called ingest test
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetInvalidInputOwnership() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);

    // null ownership params with ownership input
    final SubmitFormPromptInput promptInput =
        new SubmitFormPromptInput(
            TEST_PROMPT_ID,
            TEST_FORM_URN,
            FormPromptType.OWNERSHIP,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    final AsyncBatchSubmitFormPromptInput testInput =
        new AsyncBatchSubmitFormPromptInput(
            new ArrayList<>(),
            null,
            new ArrayList<>(),
            new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
            promptInput,
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never called ingest test
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetInvalidInputDocumentation() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);

    // null ownership params with documentation input
    final SubmitFormPromptInput promptInput =
        new SubmitFormPromptInput(
            TEST_PROMPT_ID,
            TEST_FORM_URN,
            FormPromptType.DOCUMENTATION,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    final AsyncBatchSubmitFormPromptInput testInput =
        new AsyncBatchSubmitFormPromptInput(
            new ArrayList<>(),
            null,
            new ArrayList<>(),
            new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
            promptInput,
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never called ingest test
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetInvalidInputFieldsDocumentation() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);

    // null ownership params with documentation input
    final SubmitFormPromptInput promptInput =
        new SubmitFormPromptInput(
            TEST_PROMPT_ID,
            TEST_FORM_URN,
            FormPromptType.FIELDS_DOCUMENTATION,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    final AsyncBatchSubmitFormPromptInput testInput =
        new AsyncBatchSubmitFormPromptInput(
            new ArrayList<>(),
            null,
            new ArrayList<>(),
            new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
            promptInput,
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never called ingest test
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetInputGlossaryTerms() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);
    GlossaryTermsInputParams termsInputParams = new GlossaryTermsInputParams();
    termsInputParams.setGlossaryTermUrns(ImmutableList.of("urn:li:glossaryTerm:test1"));

    // null ownership params with documentation input
    final SubmitFormPromptInput promptInput =
        new SubmitFormPromptInput(
            TEST_PROMPT_ID,
            TEST_FORM_URN,
            FormPromptType.GLOSSARY_TERMS,
            null,
            null,
            null,
            null,
            null,
            termsInputParams,
            null);
    final AsyncBatchSubmitFormPromptInput testInput =
        new AsyncBatchSubmitFormPromptInput(
            new ArrayList<>(),
            null,
            new ArrayList<>(),
            new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
            promptInput,
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    AsyncBatchSubmitFormPromptResponse response = resolver.get(mockEnv).get();

    assertEquals(response.getTaskUrn(), TEST_TASK_URN);

    // Validate that we called submit to create this test
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetInvalidInputGlossaryTerms() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);

    // null ownership params with documentation input
    final SubmitFormPromptInput promptInput =
        new SubmitFormPromptInput(
            TEST_PROMPT_ID,
            TEST_FORM_URN,
            FormPromptType.GLOSSARY_TERMS,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    final AsyncBatchSubmitFormPromptInput testInput =
        new AsyncBatchSubmitFormPromptInput(
            new ArrayList<>(),
            null,
            new ArrayList<>(),
            new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
            promptInput,
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never called ingest test
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetInputDomain() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);
    DomainInputParams inputParams = new DomainInputParams();
    inputParams.setDomainUrn("urn:li:domain:test123");

    // null ownership params with documentation input
    final SubmitFormPromptInput promptInput =
        new SubmitFormPromptInput(
            TEST_PROMPT_ID,
            TEST_FORM_URN,
            FormPromptType.DOMAIN,
            null,
            null,
            null,
            null,
            null,
            null,
            inputParams);
    final AsyncBatchSubmitFormPromptInput testInput =
        new AsyncBatchSubmitFormPromptInput(
            new ArrayList<>(),
            null,
            new ArrayList<>(),
            new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
            promptInput,
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    AsyncBatchSubmitFormPromptResponse response = resolver.get(mockEnv).get();

    assertEquals(response.getTaskUrn(), TEST_TASK_URN);

    // Validate that we called submit to create this test
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetInvalidInputDomain() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);

    // null ownership params with documentation input
    final SubmitFormPromptInput promptInput =
        new SubmitFormPromptInput(
            TEST_PROMPT_ID,
            TEST_FORM_URN,
            FormPromptType.DOMAIN,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    final AsyncBatchSubmitFormPromptInput testInput =
        new AsyncBatchSubmitFormPromptInput(
            new ArrayList<>(),
            null,
            new ArrayList<>(),
            new FormFilter(TEST_FORM_URN, TEST_USER_URN, false, false, TEST_PROMPT_ID, false),
            promptInput,
            null);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(testInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never called ingest test
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testThrowsError() throws Exception {
    FormService mockFormService = initMockFormService(false);
    EntityClient mockClient = initMockClient();
    MetadataTestClient mockTestClient = Mockito.mock(MetadataTestClient.class);
    AsyncBatchSubmitFormPromptResolver resolver =
        new AsyncBatchSubmitFormPromptResolver(mockFormService, mockClient, mockTestClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());

    // Validate that we never called ingest test since we failed earlier
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  private FormService initMockFormService(final boolean shouldSucceed) throws Exception {
    FormService service = Mockito.mock(FormService.class);

    if (!shouldSucceed) {
      Mockito.doThrow(new RuntimeException())
          .when(service)
          .getFormInfo(any(OperationContext.class), eq(UrnUtils.getUrn(TEST_FORM_URN)));
    } else {
      FormInfo formInfo = new FormInfo();
      FormActorAssignment actorAssignment = new FormActorAssignment();
      actorAssignment.setOwners(true);
      formInfo.setActors(actorAssignment);
      Mockito.when(
              service.getFormInfo(any(OperationContext.class), eq(UrnUtils.getUrn(TEST_FORM_URN))))
          .thenReturn(formInfo);
    }
    Mockito.when(
            service.getGroupsForUser(
                any(OperationContext.class), eq(UrnUtils.getUrn(TEST_USER_URN))))
        .thenReturn(new ArrayList<>());

    return service;
  }

  private EntityClient initMockClient() throws RemoteInvocationException {
    EntityClient client = Mockito.mock(EntityClient.class);

    Mockito.when(
            client.ingestProposal(
                any(OperationContext.class), any(MetadataChangeProposal.class), eq(false)))
        .thenReturn(TEST_TASK_URN);

    return client;
  }
}
