package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchSubmitFormPromptInput;
import com.linkedin.datahub.graphql.generated.DocumentationInputParams;
import com.linkedin.datahub.graphql.generated.DomainInputParams;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.GlossaryTermsInputParams;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyInputParams;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.metadata.service.FormService;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletionException;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchSubmitFormPromptResolverTest {

  private static final String TEST_DATASET_URN1 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)";
  private static final String TEST_DATASET_URN2 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,name2,PROD)";
  private static final String STRUCTURED_PROP_URN = "urn:li:structuredProperty:1";
  private static final List<String> ENTITY_URNS =
      ImmutableList.of(TEST_DATASET_URN1, TEST_DATASET_URN2);
  private static final String PROMPT_ID = "123";
  private static final String TEST_FORM_URN = "urn:li:form:1";
  private static final String TEST_DOCUMENTATION = "test documentation";
  private static final String TERM_URN = "urn:li:glossaryTerm:test1";
  private static final String DOMAIN_URN = "urn:li:domain:test";

  @Test
  public void testGetSuccess() throws Exception {
    FormService mockFormService = initMockFormService();
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateInput(false);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    // Validate that we called batchSubmitStructuredPropertyPromptResponse on the service
    Mockito.verify(mockFormService, Mockito.times(1))
        .batchSubmitStructuredPropertyPromptResponse(
            any(OperationContext.class),
            Mockito.eq(ENTITY_URNS),
            Mockito.eq(UrnUtils.getUrn(STRUCTURED_PROP_URN)),
            Mockito.eq(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("test"))),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(PROMPT_ID),
            Mockito.any(),
            Mockito.eq(true));
    Mockito.verify(mockFormService, Mockito.times(0))
        .batchSubmitFieldStructuredPropertyPromptResponse(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetFieldLevelSuccess() throws Exception {
    FormService mockFormService = initMockFormService();
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateInput(true);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    // Validate that we called batchSubmitStructuredPropertyPromptResponse on the service
    Mockito.verify(mockFormService, Mockito.times(1))
        .batchSubmitFieldStructuredPropertyPromptResponse(
            any(OperationContext.class),
            Mockito.eq(ENTITY_URNS),
            Mockito.eq(UrnUtils.getUrn(STRUCTURED_PROP_URN)),
            Mockito.eq(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("test"))),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(PROMPT_ID),
            Mockito.eq(ImmutableList.of("fieldPath1")),
            Mockito.any());
    // ensure the other form service endpoint doesn't get called
    Mockito.verify(mockFormService, Mockito.times(0))
        .batchSubmitStructuredPropertyPromptResponse(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(true));
  }

  @Test
  public void testFailure() throws Exception {
    FormService mockFormService = Mockito.mock(FormService.class);
    Mockito.when(
            mockFormService.batchSubmitStructuredPropertyPromptResponse(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(true)))
        .thenThrow(new RuntimeException());
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateInput(false);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetDocumentationSuccess() throws Exception {
    FormService mockFormService = initMockFormService();
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateDocumentationInput(true);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    // Validate we never call form service with missing params
    Mockito.verify(mockFormService, Mockito.times(1))
        .batchSubmitDocumentationPromptResponse(
            any(OperationContext.class),
            Mockito.eq(ENTITY_URNS),
            Mockito.eq(TEST_DOCUMENTATION),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(PROMPT_ID),
            Mockito.any(),
            Mockito.eq(true));
  }

  @Test
  public void testGetFailureMissingDocumentationParams() throws Exception {
    FormService mockFormService = initMockFormService();
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateDocumentationInput(false);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetGlossaryTermsSuccess() throws Exception {
    FormService mockFormService = initMockFormService();
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateGlossaryTermsInput(true);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    // Validate we never call form service with missing params
    Mockito.verify(mockFormService, Mockito.times(1))
        .batchSubmitGlossaryTermsPromptResponse(
            any(OperationContext.class),
            Mockito.eq(ENTITY_URNS),
            Mockito.eq(ImmutableList.of(UrnUtils.getUrn(TERM_URN))),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(PROMPT_ID),
            Mockito.any(),
            Mockito.eq(true));
  }

  @Test
  public void testGetFailureMissingGlossaryTermsParams() throws Exception {
    FormService mockFormService = initMockFormService();
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateGlossaryTermsInput(false);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetDomainSuccess() throws Exception {
    FormService mockFormService = initMockFormService();
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateDomainInput(true);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    boolean success = resolver.get(mockEnv).get();

    assertTrue(success);

    Mockito.verify(mockFormService, Mockito.times(1))
        .batchSubmitDomainPromptResponse(
            any(OperationContext.class),
            Mockito.eq(ENTITY_URNS),
            Mockito.eq(UrnUtils.getUrn(DOMAIN_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(PROMPT_ID),
            Mockito.any(),
            Mockito.eq(true));
  }

  @Test
  public void testGetFailureMissingDomainParams() throws Exception {
    FormService mockFormService = initMockFormService();
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateDomainInput(false);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private FormService initMockFormService() throws Exception {
    FormService service = Mockito.mock(FormService.class);
    Mockito.when(
            service.batchSubmitStructuredPropertyPromptResponse(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(true)))
        .thenReturn(true);

    Mockito.when(
            service.batchSubmitFieldStructuredPropertyPromptResponse(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(true);

    Mockito.when(
            service.batchSubmitDocumentationPromptResponse(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(true)))
        .thenReturn(true);

    Mockito.when(
            service.batchSubmitGlossaryTermsPromptResponse(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(true)))
        .thenReturn(true);

    Mockito.when(
            service.batchSubmitDomainPromptResponse(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(true)))
        .thenReturn(true);

    return service;
  }

  private BatchSubmitFormPromptInput generateInput(@Nonnull final boolean shouldAddField)
      throws Exception {
    BatchSubmitFormPromptInput input = new BatchSubmitFormPromptInput();
    input.setAssetUrns(ENTITY_URNS);
    SubmitFormPromptInput promptInput = new SubmitFormPromptInput();
    promptInput.setPromptId(PROMPT_ID);
    promptInput.setFormUrn(TEST_FORM_URN);
    StructuredPropertyInputParams propertyParams = new StructuredPropertyInputParams();
    propertyParams.setStructuredPropertyUrn(STRUCTURED_PROP_URN);
    PropertyValueInput value = new PropertyValueInput();
    value.setStringValue("test");
    propertyParams.setValues(ImmutableList.of(value));
    promptInput.setStructuredPropertyParams(propertyParams);
    if (shouldAddField) {
      promptInput.setFieldPath("fieldPath1");
      promptInput.setType(FormPromptType.FIELDS_STRUCTURED_PROPERTY);
    } else {
      promptInput.setType(FormPromptType.STRUCTURED_PROPERTY);
    }
    input.setInput(promptInput);

    return input;
  }

  private BatchSubmitFormPromptInput generateDocumentationInput(boolean addDocParams)
      throws Exception {
    BatchSubmitFormPromptInput input = new BatchSubmitFormPromptInput();
    input.setAssetUrns(ENTITY_URNS);
    SubmitFormPromptInput promptInput = new SubmitFormPromptInput();
    promptInput.setType(FormPromptType.DOCUMENTATION);
    promptInput.setPromptId(PROMPT_ID);
    promptInput.setFormUrn(TEST_FORM_URN);
    if (addDocParams) {
      DocumentationInputParams documentationParams = new DocumentationInputParams();
      documentationParams.setDocumentation(TEST_DOCUMENTATION);
      promptInput.setDocumentationParams(documentationParams);
    }
    input.setInput(promptInput);

    return input;
  }

  private BatchSubmitFormPromptInput generateGlossaryTermsInput(boolean addParams)
      throws Exception {
    BatchSubmitFormPromptInput input = new BatchSubmitFormPromptInput();
    input.setAssetUrns(ENTITY_URNS);
    SubmitFormPromptInput promptInput = new SubmitFormPromptInput();
    promptInput.setType(FormPromptType.GLOSSARY_TERMS);
    promptInput.setPromptId(PROMPT_ID);
    promptInput.setFormUrn(TEST_FORM_URN);
    if (addParams) {
      GlossaryTermsInputParams glossaryTermParams = new GlossaryTermsInputParams();
      glossaryTermParams.setGlossaryTermUrns(ImmutableList.of(TERM_URN));
      promptInput.setGlossaryTermsParams(glossaryTermParams);
    }
    input.setInput(promptInput);

    return input;
  }

  private BatchSubmitFormPromptInput generateDomainInput(boolean addParams) throws Exception {
    BatchSubmitFormPromptInput input = new BatchSubmitFormPromptInput();
    input.setAssetUrns(ENTITY_URNS);
    SubmitFormPromptInput promptInput = new SubmitFormPromptInput();
    promptInput.setType(FormPromptType.DOMAIN);
    promptInput.setPromptId(PROMPT_ID);
    promptInput.setFormUrn(TEST_FORM_URN);
    if (addParams) {
      DomainInputParams domainParams = new DomainInputParams();
      domainParams.setDomainUrn(DOMAIN_URN);
      promptInput.setDomainParams(domainParams);
    }
    input.setInput(promptInput);

    return input;
  }
}
