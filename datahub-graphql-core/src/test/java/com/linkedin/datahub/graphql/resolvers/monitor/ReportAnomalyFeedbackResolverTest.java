package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ReportAnomalyFeedbackInput;
import com.linkedin.datahub.graphql.resolvers.assertion.ReportAnomalyFeedbackResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import java.util.function.LongSupplier;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ReportAnomalyFeedbackResolverTest {

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:1234");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(String.format("urn:li:monitor:(%s,testMonitor)", TEST_ENTITY_URN));
  private static final long TEST_RUN_EVENT_TIMESTAMP = 1640000000000L;
  private static final long FIXED_TIME = 1650000000000L;

  private static final ReportAnomalyFeedbackInput TEST_INPUT =
      new ReportAnomalyFeedbackInput(
          TEST_MONITOR_URN.toString(),
          TEST_ASSERTION_URN.toString(),
          TEST_RUN_EVENT_TIMESTAMP,
          com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);

  @Test
  public void testGetSuccess() throws Exception {
    // Create mocks
    AssertionService mockAssertionService = initMockAssertionService();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    LongSupplier fixedTimeProvider = () -> FIXED_TIME;

    // Create resolver
    ReportAnomalyFeedbackResolver resolver =
        new ReportAnomalyFeedbackResolver(
            mockAssertionService, mockMonitorService, mockEntityClient, fixedTimeProvider);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent result = resolver.get(mockEnv).get();

    // Verify result
    assertEquals(
        result.getState(), com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);
    assertEquals(result.getTimestampMillis(), FIXED_TIME);
    assertEquals(result.getSource().getSourceUrn(), TEST_ASSERTION_URN.toString());
    assertEquals(result.getSource().getSourceEventTimestampMillis(), TEST_RUN_EVENT_TIMESTAMP);

    // Verify ingestProposal was called with correct params
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(
                proposal ->
                    proposal.getEntityUrn().equals(TEST_MONITOR_URN)
                        && proposal
                            .getAspectName()
                            .equals(Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME)),
            Mockito.eq(false));

    // Verify retraining called
    Mockito.verify(mockMonitorService, Mockito.times(1)).retrainAssertionMonitor(TEST_MONITOR_URN);
  }

  @Test
  public void testGetWithMissingRunEvent() throws Exception {
    // Create mocks with null run event
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    Mockito.when(
            mockAssertionService.getAssertionRunEvent(
                any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(TEST_RUN_EVENT_TIMESTAMP)))
        .thenReturn(null);

    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Create resolver
    ReportAnomalyFeedbackResolver resolver =
        new ReportAnomalyFeedbackResolver(
            mockAssertionService, mockMonitorService, mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Should throw an exception due to missing run event
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetWithIngestError() throws Exception {
    // Create mocks
    AssertionService mockAssertionService = initMockAssertionService();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Configure entity client to throw exception
    Mockito.doThrow(new RemoteInvocationException("Ingest error"))
        .when(mockEntityClient)
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));

    // Create resolver
    ReportAnomalyFeedbackResolver resolver =
        new ReportAnomalyFeedbackResolver(
            mockAssertionService, mockMonitorService, mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Should throw exception due to ingest error
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private AssertionService initMockAssertionService() {
    AssertionService service = Mockito.mock(AssertionService.class);

    // Create a mock run event
    AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setTimestampMillis(TEST_RUN_EVENT_TIMESTAMP);
    runEvent.setResult(new AssertionResult());

    Mockito.when(
            service.getAssertionRunEvent(
                any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(TEST_RUN_EVENT_TIMESTAMP)))
        .thenReturn(runEvent);

    return service;
  }

  private MonitorService initMockMonitorService() {
    return Mockito.mock(MonitorService.class);
  }
}
