package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultError;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssertionResultErrorInput;
import com.linkedin.datahub.graphql.generated.AssertionResultErrorType;
import com.linkedin.datahub.graphql.generated.AssertionResultInput;
import com.linkedin.datahub.graphql.generated.AssertionResultSeverity;
import com.linkedin.datahub.graphql.generated.AssertionResultType;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.metadata.service.AssertionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ReportAssertionResultResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");

  private static final String customAssertionUrl = "https://dq-platform-native-url";

  private static final AssertionResultInput TEST_INPUT =
      new AssertionResultInput(
          0L,
          AssertionResultType.ERROR,
          ImmutableList.of(new StringMapEntryInput("prop1", "value1")),
          customAssertionUrl,
          new AssertionResultErrorInput(
              AssertionResultErrorType.UNKNOWN_ERROR, "an unknown error occurred"),
          null,
          null,
          null,
          null,
          null);

  private static final AssertionResultInput TEST_FAILURE_INPUT_WITH_SEVERITY =
      new AssertionResultInput(
          0L,
          AssertionResultType.FAILURE,
          ImmutableList.of(new StringMapEntryInput("prop1", "value1")),
          customAssertionUrl,
          null,
          AssertionResultSeverity.HIGH,
          null,
          null,
          null,
          null);

  private static final AssertionResultInput TEST_SUCCESS_INPUT_WITH_SEVERITY =
      new AssertionResultInput(
          0L,
          AssertionResultType.SUCCESS,
          ImmutableList.of(new StringMapEntryInput("prop1", "value1")),
          customAssertionUrl,
          null,
          AssertionResultSeverity.HIGH,
          null,
          null,
          null,
          null);

  private static final AssertionRunEvent TEST_ASSERTION_RUN_EVENT =
      new AssertionRunEvent()
          .setAssertionUrn(TEST_ASSERTION_URN)
          .setAsserteeUrn(TEST_DATASET_URN)
          .setTimestampMillis(0L)
          .setRunId("0")
          .setStatus(AssertionRunStatus.COMPLETE)
          .setResult(
              new AssertionResult()
                  .setType(com.linkedin.assertion.AssertionResultType.ERROR)
                  .setError(
                      new AssertionResultError()
                          .setType(com.linkedin.assertion.AssertionResultErrorType.UNKNOWN_ERROR)
                          .setProperties(
                              new StringMap(Map.of("message", "an unknown error occurred"))))
                  .setExternalUrl(customAssertionUrl)
                  .setNativeResults(new StringMap(Map.of("prop1", "value1"))));

  @Test
  public void testGetSuccessReportAssertionRunEvent() throws Exception {
    // Update resolver
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    ReportAssertionResultResolver resolver = new ReportAssertionResultResolver(mockedService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("result"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockedService.getEntityUrnForAssertion(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(TEST_DATASET_URN);

    resolver.get(mockEnv).get();

    // Validate that we created the assertion
    Mockito.verify(mockedService, Mockito.times(1))
        .addAssertionRunEvent(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_ASSERTION_RUN_EVENT.getTimestampMillis()),
            Mockito.eq(TEST_ASSERTION_RUN_EVENT.getResult()));
  }

  @Test
  public void testGetUpdateAssertionUnauthorized() throws Exception {
    // Update resolver
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    ReportAssertionResultResolver resolver = new ReportAssertionResultResolver(mockedService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("result"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockedService.getEntityUrnForAssertion(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(TEST_DATASET_URN);

    CompletionException e =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    assert e.getMessage()
        .contains(
            "Unauthorized to perform this action. Please contact your DataHub administrator.");

    // Validate that we created the assertion
    Mockito.verify(mockedService, Mockito.times(0))
        .addAssertionRunEvent(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetAssertionServiceException() {
    // Update resolver
    AssertionService mockService = Mockito.mock(AssertionService.class);

    Mockito.when(
            mockService.getEntityUrnForAssertion(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(TEST_DATASET_URN);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .addAssertionRunEvent(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());

    ReportAssertionResultResolver resolver = new ReportAssertionResultResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("result"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testReportAssertionFailureWithSeverity() throws Exception {
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    ReportAssertionResultResolver resolver = new ReportAssertionResultResolver(mockedService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("result")))
        .thenReturn(TEST_FAILURE_INPUT_WITH_SEVERITY);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockedService.getEntityUrnForAssertion(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(TEST_DATASET_URN);

    resolver.get(mockEnv).get();

    AssertionResult expectedResult =
        new AssertionResult()
            .setType(com.linkedin.assertion.AssertionResultType.FAILURE)
            .setSeverity(com.linkedin.assertion.AssertionResultSeverity.HIGH)
            .setExternalUrl(customAssertionUrl)
            .setNativeResults(new StringMap(Map.of("prop1", "value1")));

    Mockito.verify(mockedService, Mockito.times(1))
        .addAssertionRunEvent(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(0L),
            Mockito.eq(expectedResult));
  }

  @Test
  public void testReportAssertionSuccessIgnoresSeverity() throws Exception {
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    ReportAssertionResultResolver resolver = new ReportAssertionResultResolver(mockedService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("result")))
        .thenReturn(TEST_SUCCESS_INPUT_WITH_SEVERITY);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockedService.getEntityUrnForAssertion(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(TEST_DATASET_URN);

    resolver.get(mockEnv).get();

    AssertionResult expectedResult =
        new AssertionResult()
            .setType(com.linkedin.assertion.AssertionResultType.SUCCESS)
            .setExternalUrl(customAssertionUrl)
            .setNativeResults(new StringMap(Map.of("prop1", "value1")));

    Mockito.verify(mockedService, Mockito.times(1))
        .addAssertionRunEvent(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(0L),
            Mockito.eq(expectedResult));
  }

  @Test
  public void testReportAssertionFailureWithNumericValues() throws Exception {
    // graphql-java hands bindArgument a Map with a Double; Jackson narrows it to Float.
    AssertionResult result =
        reportWithRawInput(
            Map.of(
                "timestampMillis", 0L,
                "type", "FAILURE",
                "actualAggValue", 0.12d,
                "rowCount", 1000L,
                "unexpectedCount", 7L,
                "missingCount", 120L));

    assertEquals(
        result,
        new AssertionResult()
            .setType(com.linkedin.assertion.AssertionResultType.FAILURE)
            .setActualAggValue(0.12f)
            .setRowCount(1000L)
            .setUnexpectedCount(7L)
            .setMissingCount(120L));
  }

  @Test
  public void testReportAssertionSuccessKeepsNumericValues() throws Exception {
    AssertionResult result =
        reportWithRawInput(
            Map.of(
                "timestampMillis",
                0L,
                "type",
                "SUCCESS",
                "actualAggValue",
                42.5d,
                "rowCount",
                1000L));

    assertEquals(
        result,
        new AssertionResult()
            .setType(com.linkedin.assertion.AssertionResultType.SUCCESS)
            .setActualAggValue(42.5f)
            .setRowCount(1000L));
  }

  @Test
  public void testReportAssertionErrorIgnoresNumericValues() throws Exception {
    AssertionResult result =
        reportWithRawInput(
            Map.of(
                "timestampMillis", 0L,
                "type", "ERROR",
                "actualAggValue", 42.5d,
                "rowCount", 1000L,
                "unexpectedCount", 7L,
                "missingCount", 3L));

    assertEquals(
        result, new AssertionResult().setType(com.linkedin.assertion.AssertionResultType.ERROR));
  }

  @Test
  public void testReportAssertionRejectsActualAggValueOutsideFloatRange() throws Exception {
    // Control: the same input shape binds and is stored at the largest finite float, so the
    // failure below comes from the range guard rather than from argument binding.
    AssertionResult stored =
        reportWithRawInput(
            Map.of(
                "timestampMillis",
                0L,
                "type",
                "FAILURE",
                "actualAggValue",
                (double) Float.MAX_VALUE));
    assertEquals(stored.getActualAggValue(), Float.MAX_VALUE);

    AssertionService mockedService = Mockito.mock(AssertionService.class);
    Mockito.when(
            mockedService.getEntityUrnForAssertion(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(TEST_DATASET_URN);
    DataFetchingEnvironment mockEnv =
        mockEnvWithRawInput(
            Map.of("timestampMillis", 0L, "type", "FAILURE", "actualAggValue", 1e39d));

    Throwable thrown =
        expectThrows(
            Throwable.class,
            () -> new ReportAssertionResultResolver(mockedService).get(mockEnv).join());
    Throwable cause = thrown instanceof CompletionException ? thrown.getCause() : thrown;
    assertTrue(cause instanceof IllegalArgumentException);
    Mockito.verify(mockedService, Mockito.never())
        .addAssertionRunEvent(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());
  }

  private static DataFetchingEnvironment mockEnvWithRawInput(Map<String, Object> input) {
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("result"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    return mockEnv;
  }

  private static AssertionResult reportWithRawInput(Map<String, Object> input) throws Exception {
    AssertionService mockedService = Mockito.mock(AssertionService.class);
    Mockito.when(
            mockedService.getEntityUrnForAssertion(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(TEST_DATASET_URN);

    new ReportAssertionResultResolver(mockedService).get(mockEnvWithRawInput(input)).get();

    ArgumentCaptor<AssertionResult> captor = ArgumentCaptor.forClass(AssertionResult.class);
    Mockito.verify(mockedService, Mockito.times(1))
        .addAssertionRunEvent(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(0L),
            captor.capture());
    return captor.getValue();
  }
}
