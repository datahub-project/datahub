package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ReportAnomalyFeedbackInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
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
  private static final long LATEST_ANOMALY_TIMESTAMP =
      1645000000000L; // between run event and current time
  private static final long FUTURE_ANOMALY_TIMESTAMP = 1655000000000L; // after current time

  private static final ReportAnomalyFeedbackInput TEST_INPUT =
      new ReportAnomalyFeedbackInput(
          TEST_MONITOR_URN.toString(),
          TEST_ASSERTION_URN.toString(),
          TEST_RUN_EVENT_TIMESTAMP,
          com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);

  @Test
  public void testGetSuccessWithCurrentTimeGreaterThanLatestAnomaly() throws Exception {
    // Create mocks - latest anomaly timestamp is less than current time
    AssertionService mockAssertionService = initMockAssertionService();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = initMockEntityClient(LATEST_ANOMALY_TIMESTAMP);
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

    // Verify result - should use FIXED_TIME since it's greater than LATEST_ANOMALY_TIMESTAMP
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
  public void testGetSuccessWithLatestAnomalyGreaterThanCurrentTime() throws Exception {
    // Create mocks - latest anomaly timestamp is greater than current time
    AssertionService mockAssertionService = initMockAssertionService();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = initMockEntityClient(FUTURE_ANOMALY_TIMESTAMP);
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

    // Verify result - should use FUTURE_ANOMALY_TIMESTAMP since it's greater than FIXED_TIME
    assertEquals(
        result.getState(), com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);
    assertEquals(result.getTimestampMillis(), FUTURE_ANOMALY_TIMESTAMP);
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
  public void testGetSuccessWithNoExistingAnomalies() throws Exception {
    // Create mocks - no existing anomalies (empty list)
    AssertionService mockAssertionService = initMockAssertionService();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = initMockEntityClientWithNoAnomalies();
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

    // Verify result - should use FIXED_TIME since there are no existing anomalies (returns 0)
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
  public void testGetWithFailedLatestAnomalyFetch() throws Exception {
    // Create mocks - getTimeseriesAspectValues throws exception
    AssertionService mockAssertionService = initMockAssertionService();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Mock getTimeseriesAspectValues to throw exception
    Mockito.when(
            mockEntityClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN.toString()),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null)))
        .thenThrow(new RemoteInvocationException("Failed to fetch"));

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

    // Verify result - should fall back to FIXED_TIME on error
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
    EntityClient mockEntityClient = initMockEntityClient(LATEST_ANOMALY_TIMESTAMP);

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
    EntityClient mockEntityClient = initMockEntityClient(LATEST_ANOMALY_TIMESTAMP);

    // Configure entity client to throw exception on ingest
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

  private EntityClient initMockEntityClient(long anomalyTimestamp) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    // Create a mock anomaly event with the specified timestamp
    MonitorAnomalyEvent mockAnomalyEvent = new MonitorAnomalyEvent();
    mockAnomalyEvent.setTimestampMillis(anomalyTimestamp);

    // Create enveloped aspect with the anomaly event
    GenericAspect genericAspect = GenericRecordUtils.serializeAspect(mockAnomalyEvent);
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setAspect(genericAspect);

    // Mock getTimeseriesAspectValues to return the mock anomaly event
    Mockito.when(
            client.getTimeseriesAspectValues(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN.toString()),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null)))
        .thenReturn(List.of(envelopedAspect));

    return client;
  }

  private EntityClient initMockEntityClientWithNoAnomalies() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    // Mock getTimeseriesAspectValues to return an empty list (no existing anomalies)
    Mockito.when(
            client.getTimeseriesAspectValues(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN.toString()),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null)))
        .thenReturn(Collections.emptyList());

    return client;
  }
}
