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
import com.linkedin.datahub.graphql.generated.AssertionResultType;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.metadata.service.AssertionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.concurrent.CompletionException;
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
              AssertionResultErrorType.UNKNOWN_ERROR, "an unknown error occurred"));

  ;

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
}
