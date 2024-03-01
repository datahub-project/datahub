package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchSubmitFormPromptInput;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyInputParams;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.metadata.service.FormService;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import graphql.schema.DataFetchingEnvironment;
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
            Mockito.eq(ENTITY_URNS),
            Mockito.eq(UrnUtils.getUrn(STRUCTURED_PROP_URN)),
            Mockito.eq(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("test"))),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(PROMPT_ID),
            Mockito.any(Authentication.class));
    Mockito.verify(mockFormService, Mockito.times(0))
        .batchSubmitFieldStructuredPropertyPromptResponse(
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
            Mockito.eq(ENTITY_URNS),
            Mockito.eq(UrnUtils.getUrn(STRUCTURED_PROP_URN)),
            Mockito.eq(new PrimitivePropertyValueArray(PrimitivePropertyValue.create("test"))),
            Mockito.eq(UrnUtils.getUrn(TEST_FORM_URN)),
            Mockito.eq(PROMPT_ID),
            Mockito.eq("fieldPath1"),
            Mockito.any(Authentication.class));
    // ensure the other form service endpoint doesn't get called
    Mockito.verify(mockFormService, Mockito.times(0))
        .batchSubmitStructuredPropertyPromptResponse(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testFailure() throws Exception {
    FormService mockFormService = Mockito.mock(FormService.class);
    Mockito.when(
            mockFormService.batchSubmitStructuredPropertyPromptResponse(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(Authentication.class)))
        .thenThrow(new RuntimeException());
    BatchSubmitFormPromptResolver resolver = new BatchSubmitFormPromptResolver(mockFormService);

    BatchSubmitFormPromptInput input = generateInput(false);
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
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(Authentication.class)))
        .thenReturn(true);

    Mockito.when(
            service.batchSubmitFieldStructuredPropertyPromptResponse(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(Authentication.class)))
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
}
