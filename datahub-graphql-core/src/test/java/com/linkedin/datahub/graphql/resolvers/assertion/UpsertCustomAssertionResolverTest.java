package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.PlatformInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpsertCustomAssertionInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.AssertionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpsertCustomAssertionResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");

  private static final String TEST_INVALID_DATASET_URN = "dataset.name";

  private static final Urn TEST_FIELD_URN =
      UrnUtils.getUrn(
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD),field1)");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");

  private static final String TEST_INVALID_ASSERTION_URN = "test";
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:actor:test");

  private static final Urn TEST_PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:DQplatform");

  private static final String customAssertionType = "My custom category";
  private static final String customAssertionDescription = "Description of custom assertion";
  private static final String customAssertionUrl = "https://dq-platform-native-url";

  private static final String customAssertionLogic = "custom script of assertion";

  private static final UpsertCustomAssertionInput TEST_INPUT =
      UpsertCustomAssertionInput.builder()
          .setEntityUrn(TEST_DATASET_URN.toString())
          .setType(customAssertionType)
          .setDescription(customAssertionDescription)
          .setFieldPath("field1")
          .setPlatform(new PlatformInput(null, "DQplatform"))
          .setExternalUrl(customAssertionUrl)
          .setLogic(customAssertionLogic)
          .build();

  private static final UpsertCustomAssertionInput TEST_INPUT_MISSING_PLATFORM =
      UpsertCustomAssertionInput.builder()
          .setEntityUrn(TEST_DATASET_URN.toString())
          .setType(customAssertionType)
          .setDescription(customAssertionDescription)
          .setFieldPath("field1")
          .setPlatform(new PlatformInput(null, null))
          .setExternalUrl(customAssertionUrl)
          .setLogic(customAssertionLogic)
          .build();

  private static final UpsertCustomAssertionInput TEST_INPUT_BLANK_FIELD_PATH =
      UpsertCustomAssertionInput.builder()
          .setEntityUrn(TEST_DATASET_URN.toString())
          .setType(customAssertionType)
          .setDescription(customAssertionDescription)
          .setFieldPath("   ")
          .setPlatform(new PlatformInput(null, "DQplatform"))
          .setExternalUrl(customAssertionUrl)
          .setLogic(customAssertionLogic)
          .build();

  private static final UpsertCustomAssertionInput TEST_INPUT_BLANK_FIELD_PATHS_ENTRY =
      UpsertCustomAssertionInput.builder()
          .setEntityUrn(TEST_DATASET_URN.toString())
          .setType(customAssertionType)
          .setDescription(customAssertionDescription)
          .setFieldPaths(List.of("field1", "   "))
          .setPlatform(new PlatformInput(null, "DQplatform"))
          .setExternalUrl(customAssertionUrl)
          .setLogic(customAssertionLogic)
          .build();

  private static final UpsertCustomAssertionInput TEST_INPUT_INVALID_ENTITY_URN =
      UpsertCustomAssertionInput.builder()
          .setEntityUrn(TEST_INVALID_DATASET_URN)
          .setType(customAssertionType)
          .setDescription(customAssertionDescription)
          .setFieldPath("field1")
          .setPlatform(new PlatformInput(null, "DQplatform"))
          .setExternalUrl(customAssertionUrl)
          .setLogic(customAssertionLogic)
          .build();

  private static final UpsertCustomAssertionInput TEST_INPUT_STRUCTURED =
      UpsertCustomAssertionInput.builder()
          .setEntityUrn(TEST_DATASET_URN.toString())
          .setType(customAssertionType)
          .setDescription(customAssertionDescription)
          .setFieldPaths(List.of("field1"))
          .setPlatform(new PlatformInput(null, "DQplatform"))
          .setExternalUrl(customAssertionUrl)
          .setLogic(customAssertionLogic)
          .setScope(com.linkedin.datahub.graphql.generated.DatasetAssertionScope.DATASET_COLUMN)
          .setAggregation(com.linkedin.datahub.graphql.generated.AssertionStdAggregation.IDENTITY)
          .setOperator(com.linkedin.datahub.graphql.generated.AssertionStdOperator.NOT_NULL)
          .setParameters(
              AssertionStdParametersInput.builder()
                  .setValue(
                      AssertionStdParameterInput.builder()
                          .setType(
                              com.linkedin.datahub.graphql.generated.AssertionStdParameterType
                                  .NUMBER)
                          .setValue("1")
                          .build())
                  .build())
          .setNativeType("not_null")
          .setNativeParameters(List.of(new StringMapEntryInput("column_name", "field1")))
          .build();

  private static final AssertionInfo TEST_ASSERTION_INFO =
      new AssertionInfo()
          .setType(AssertionType.CUSTOM)
          .setDescription(customAssertionDescription)
          .setExternalUrl(new Url(customAssertionUrl))
          .setSource(
              new AssertionSource()
                  .setType(AssertionSourceType.EXTERNAL)
                  .setCreated(
                      new AuditStamp()
                          .setTime(System.currentTimeMillis())
                          .setActor(TEST_ACTOR_URN)))
          .setCustomAssertion(
              new CustomAssertionInfo()
                  .setEntity(TEST_DATASET_URN)
                  .setType(customAssertionType)
                  .setField(TEST_FIELD_URN)
                  .setFields(new UrnArray(TEST_FIELD_URN))
                  .setLogic(customAssertionLogic));

  private static final DataPlatformInstance TEST_DATA_PLATFORM_INSTANCE =
      new DataPlatformInstance().setPlatform(TEST_PLATFORM_URN);

  @Test
  public void testGetSuccessCreateAssertion() throws Exception {
    // Update resolver
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(mockedService.generateAssertionUrn()).thenReturn(TEST_ASSERTION_URN);
    Mockito.when(
            mockedService.getAssertionEntityResponse(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_INFO.data())),
                            Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(TEST_DATA_PLATFORM_INSTANCE.data())))))
                .setEntityName(Constants.ASSERTION_ENTITY_NAME)
                .setUrn(TEST_ASSERTION_URN));

    Assertion assertion = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(assertion);
    assertEquals(assertion.getUrn(), TEST_ASSERTION_URN.toString());

    // Validate that we created the assertion
    Mockito.verify(mockedService, Mockito.times(1))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_ASSERTION_INFO.getCustomAssertion().getEntity()),
            Mockito.eq(TEST_ASSERTION_INFO.getDescription()),
            Mockito.eq(TEST_ASSERTION_INFO.getExternalUrl().toString()),
            Mockito.eq(TEST_DATA_PLATFORM_INSTANCE),
            Mockito.eq(TEST_ASSERTION_INFO.getCustomAssertion()));
  }

  @Test
  public void testGetSuccessUpdateAssertion() throws Exception {
    // Update resolver
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockedService.getAssertionEntityResponse(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_INFO.data())),
                            Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(TEST_DATA_PLATFORM_INSTANCE.data())))))
                .setEntityName(Constants.ASSERTION_ENTITY_NAME)
                .setUrn(TEST_ASSERTION_URN));

    Assertion assertion = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(assertion);
    assertEquals(assertion.getUrn(), TEST_ASSERTION_URN.toString());

    // Validate that we created the assertion
    Mockito.verify(mockedService, Mockito.times(1))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_ASSERTION_INFO.getCustomAssertion().getEntity()),
            Mockito.eq(TEST_ASSERTION_INFO.getDescription()),
            Mockito.eq(TEST_ASSERTION_INFO.getExternalUrl().toString()),
            Mockito.eq(TEST_DATA_PLATFORM_INSTANCE),
            Mockito.eq(TEST_ASSERTION_INFO.getCustomAssertion()));
  }

  @Test
  public void testGetSuccessCreateAssertionWithStructuredFields() throws Exception {
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_STRUCTURED);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(mockedService.generateAssertionUrn()).thenReturn(TEST_ASSERTION_URN);
    Mockito.when(
            mockedService.getAssertionEntityResponse(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_INFO.data())),
                            Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(TEST_DATA_PLATFORM_INSTANCE.data())))))
                .setEntityName(Constants.ASSERTION_ENTITY_NAME)
                .setUrn(TEST_ASSERTION_URN));

    Assertion assertion = resolver.get(mockEnv).get();
    assertNotNull(assertion);
    assertEquals(assertion.getUrn(), TEST_ASSERTION_URN.toString());

    ArgumentCaptor<CustomAssertionInfo> customAssertionCaptor =
        ArgumentCaptor.forClass(CustomAssertionInfo.class);
    Mockito.verify(mockedService, Mockito.times(1))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(customAssertionDescription),
            Mockito.eq(customAssertionUrl),
            Mockito.eq(TEST_DATA_PLATFORM_INSTANCE),
            customAssertionCaptor.capture());

    CustomAssertionInfo customAssertion = customAssertionCaptor.getValue();
    assertEquals(customAssertion.getType(), customAssertionType);
    assertEquals(customAssertion.getEntity(), TEST_DATASET_URN);
    assertEquals(customAssertion.getField(), TEST_FIELD_URN);
    assertEquals(customAssertion.getFields(), new UrnArray(TEST_FIELD_URN));
    assertEquals(customAssertion.getLogic(), customAssertionLogic);
    assertEquals(customAssertion.getScope(), DatasetAssertionScope.DATASET_COLUMN);
    assertEquals(customAssertion.getAggregation(), AssertionStdAggregation.IDENTITY);
    assertEquals(customAssertion.getOperator(), AssertionStdOperator.NOT_NULL);
    assertEquals(customAssertion.getNativeType(), "not_null");
    assertEquals(
        customAssertion.getParameters(),
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1")));
    StringMap expectedNativeParameters = new StringMap();
    expectedNativeParameters.put("column_name", "field1");
    assertEquals(customAssertion.getNativeParameters(), expectedNativeParameters);
  }

  @Test
  public void testGetUpdateAssertionUnauthorized() throws Exception {
    // Update resolver
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    CompletionException e =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    assert e.getMessage()
        .contains(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");

    Mockito.verify(mockedService, Mockito.times(0))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetUpsertAssertionMissingPlatformFailure() throws Exception {
    // Update resolver
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_MISSING_PLATFORM);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    CompletionException e =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    assert e.getMessage()
        .contains(
            "Failed to upsert Custom Assertion. Platform Name or Platform Urn must be specified.");

    Mockito.verify(mockedService, Mockito.times(0))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetUpsertAssertionBlankFieldPathFailure() throws Exception {
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_BLANK_FIELD_PATH);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    CompletionException e =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    assert e.getMessage().contains("fieldPath must not be blank when provided");

    Mockito.verify(mockedService, Mockito.times(0))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetUpsertAssertionBlankFieldPathsEntryFailure() throws Exception {
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(TEST_INPUT_BLANK_FIELD_PATHS_ENTRY);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    CompletionException e =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    assert e.getMessage().contains("fieldPaths must not contain blank entries");

    Mockito.verify(mockedService, Mockito.times(0))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetUpsertAssertionInvalidAssertionUrn() throws Exception {
    // Update resolver
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_INVALID_ASSERTION_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RuntimeException e = expectThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    assert e.getMessage().contains("invalid urn");

    Mockito.verify(mockedService, Mockito.times(0))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetUpsertAssertionInvalidEntityUrn() throws Exception {
    // Update resolver
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockedService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(TEST_INPUT_INVALID_ENTITY_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    RuntimeException e = expectThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    assert e.getMessage().contains("invalid urn");

    Mockito.verify(mockedService, Mockito.times(0))
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetAssertionServiceException() {
    // Update resolver
    AssertionService mockService = Mockito.mock(AssertionService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .upsertCustomAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());

    UpsertCustomAssertionResolver resolver = new UpsertCustomAssertionResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
