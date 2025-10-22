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
import com.linkedin.datahub.graphql.generated.BulkUpdateAnomaliesInput;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.LongSupplier;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BulkUpdateAnomaliesResolverTest {

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:1234");
  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(String.format("urn:li:monitor:(%s,testMonitor)", TEST_ENTITY_URN));
  private static final long TEST_START_TIME = 1640000000000L;
  private static final long TEST_END_TIME = 1650000000000L;
  private static final long FIXED_TIME = 1655000000000L;
  private static final long LATEST_ANOMALY_TIMESTAMP = 1654000000000L;

  private static final BulkUpdateAnomaliesInput TEST_INPUT =
      new BulkUpdateAnomaliesInput(
          TEST_MONITOR_URN.toString(),
          TEST_ASSERTION_URN.toString(),
          TEST_START_TIME,
          TEST_END_TIME,
          com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);

  @Test
  public void testGetSuccess() throws Exception {
    // Create mocks
    AssertionService mockAssertionService = initMockAssertionServiceWithMultipleEvents();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Mock getTimeseriesAspectValues to return the latest anomaly event
    mockLatestAnomalyEventTimestamp(mockEntityClient, LATEST_ANOMALY_TIMESTAMP);

    LongSupplier fixedTimeProvider = () -> FIXED_TIME;

    // Create resolver
    BulkUpdateAnomaliesResolver resolver =
        new BulkUpdateAnomaliesResolver(
            mockAssertionService, mockMonitorService, mockEntityClient, fixedTimeProvider);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    List<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent> results =
        resolver.get(mockEnv).get();

    // Verify results
    assertEquals(results.size(), 3);

    // Verify that timestamps increment from the max of current time and latest anomaly event
    // timestamp
    // FIXED_TIME (1655000000000L) > LATEST_ANOMALY_TIMESTAMP (1654000000000L), so should use
    // FIXED_TIME
    assertEquals(results.get(0).getTimestampMillis(), FIXED_TIME + 1);
    assertEquals(results.get(1).getTimestampMillis(), FIXED_TIME + 2);
    assertEquals(results.get(2).getTimestampMillis(), FIXED_TIME + 3);

    // Verify other properties
    for (com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent result : results) {
      assertEquals(
          result.getState(), com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);
      assertEquals(result.getSource().getSourceUrn(), TEST_ASSERTION_URN.toString());
    }

    // Verify getTimeseriesAspectValues was called to fetch latest timestamp
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .getTimeseriesAspectValues(
            any(OperationContext.class),
            Mockito.eq(TEST_MONITOR_URN.toString()),
            Mockito.eq(Constants.MONITOR_ENTITY_NAME),
            Mockito.eq(Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME),
            Mockito.isNull(),
            Mockito.isNull(),
            Mockito.eq(1),
            Mockito.isNull());

    // Verify batchIngestProposals was called
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(list -> list.size() == 3),
            Mockito.eq(false));

    // Verify retraining called
    Mockito.verify(mockMonitorService, Mockito.times(1)).retrainAssertionMonitor(TEST_MONITOR_URN);
  }

  @Test
  public void testGetWithNoRunEvents() throws Exception {
    // Create mocks with empty run events
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    Mockito.when(
            mockAssertionService.getAssertionRunEvents(
                any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(TEST_START_TIME),
                Mockito.eq(TEST_END_TIME)))
        .thenReturn(Collections.emptyList());

    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Mock getTimeseriesAspectValues - won't be called since there are no run events
    mockLatestAnomalyEventTimestamp(mockEntityClient, LATEST_ANOMALY_TIMESTAMP);

    // Create resolver
    BulkUpdateAnomaliesResolver resolver =
        new BulkUpdateAnomaliesResolver(mockAssertionService, mockMonitorService, mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    List<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent> results =
        resolver.get(mockEnv).get();

    // Should return empty list
    assertTrue(results.isEmpty());

    // Verify getTimeseriesAspectValues was not called since there are no run events
    Mockito.verify(mockEntityClient, Mockito.never())
        .getTimeseriesAspectValues(
            any(OperationContext.class),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());

    // Verify batchIngestProposals was not called
    Mockito.verify(mockEntityClient, Mockito.never())
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.anyBoolean());

    // Verify retraining was not called
    Mockito.verify(mockMonitorService, Mockito.never()).retrainAssertionMonitor(Mockito.any());
  }

  @Test
  public void testGetWithIngestError() throws Exception {
    // Create mocks
    AssertionService mockAssertionService = initMockAssertionServiceWithMultipleEvents();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Mock getTimeseriesAspectValues to return the latest anomaly event
    mockLatestAnomalyEventTimestamp(mockEntityClient, LATEST_ANOMALY_TIMESTAMP);

    // Configure entity client to throw exception on batch ingest
    Mockito.doThrow(new RemoteInvocationException("Ingest error"))
        .when(mockEntityClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    // Create resolver
    BulkUpdateAnomaliesResolver resolver =
        new BulkUpdateAnomaliesResolver(mockAssertionService, mockMonitorService, mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Should throw exception due to ingest error
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetWithSingleRunEvent() throws Exception {
    // Create mocks with single event
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setTimestampMillis(1645000000000L);
    runEvent.setResult(new AssertionResult());

    Mockito.when(
            mockAssertionService.getAssertionRunEvents(
                any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(TEST_START_TIME),
                Mockito.eq(TEST_END_TIME)))
        .thenReturn(Collections.singletonList(runEvent));

    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    LongSupplier fixedTimeProvider = () -> FIXED_TIME;

    // Mock getTimeseriesAspectValues to return the latest anomaly event
    mockLatestAnomalyEventTimestamp(mockEntityClient, LATEST_ANOMALY_TIMESTAMP);

    // Create resolver with fixed time provider
    BulkUpdateAnomaliesResolver resolver =
        new BulkUpdateAnomaliesResolver(
            mockAssertionService, mockMonitorService, mockEntityClient, fixedTimeProvider);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    List<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent> results =
        resolver.get(mockEnv).get();

    // Verify single result
    assertEquals(results.size(), 1);

    // Verify timestamp is incremented from max of current time and latest anomaly timestamp
    // FIXED_TIME (1655000000000L) > LATEST_ANOMALY_TIMESTAMP (1654000000000L), so should use
    // FIXED_TIME
    assertEquals(results.get(0).getTimestampMillis(), FIXED_TIME + 1);

    // Verify batchIngestProposals was called with single MCP
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(list -> list.size() == 1),
            Mockito.eq(false));
  }

  private AssertionService initMockAssertionServiceWithMultipleEvents() {
    AssertionService service = Mockito.mock(AssertionService.class);

    // Create mock run events
    AssertionRunEvent runEvent1 = new AssertionRunEvent();
    runEvent1.setTimestampMillis(1641000000000L);
    runEvent1.setResult(new AssertionResult());

    AssertionRunEvent runEvent2 = new AssertionRunEvent();
    runEvent2.setTimestampMillis(1645000000000L);
    runEvent2.setResult(new AssertionResult());

    AssertionRunEvent runEvent3 = new AssertionRunEvent();
    runEvent3.setTimestampMillis(1649000000000L);
    runEvent3.setResult(new AssertionResult());

    Mockito.when(
            service.getAssertionRunEvents(
                any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(TEST_START_TIME),
                Mockito.eq(TEST_END_TIME)))
        .thenReturn(Arrays.asList(runEvent1, runEvent2, runEvent3));

    return service;
  }

  private MonitorService initMockMonitorService() {
    return Mockito.mock(MonitorService.class);
  }

  /**
   * Mocks the EntityClient to return a MonitorAnomalyEvent with the specified timestamp. This
   * simulates fetching the latest anomaly event timestamp from the timeseries aspect store.
   */
  private void mockLatestAnomalyEventTimestamp(EntityClient mockEntityClient, long timestamp)
      throws RemoteInvocationException {
    // Create a mock anomaly event with the specified timestamp
    MonitorAnomalyEvent mockAnomalyEvent = new MonitorAnomalyEvent();
    mockAnomalyEvent.setTimestampMillis(timestamp);

    // Create enveloped aspect with the anomaly event
    GenericAspect genericAspect = GenericRecordUtils.serializeAspect(mockAnomalyEvent);
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setAspect(genericAspect);

    // Mock the getTimeseriesAspectValues call
    Mockito.when(
            mockEntityClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN.toString()),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                Mockito.isNull(),
                Mockito.isNull(),
                Mockito.eq(1),
                Mockito.isNull()))
        .thenReturn(Collections.singletonList(envelopedAspect));
  }

  @Test
  public void testGetWithLatestTimestampHigherThanCurrentTime() throws Exception {
    // Create mocks
    AssertionService mockAssertionService = initMockAssertionServiceWithMultipleEvents();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);

    // Set a timestamp that is HIGHER than the current time
    long higherTimestamp = FIXED_TIME + 1000000L; // Higher than FIXED_TIME
    LongSupplier fixedTimeProvider = () -> FIXED_TIME;

    // Mock getTimeseriesAspectValues to return a timestamp higher than current time
    mockLatestAnomalyEventTimestamp(mockEntityClient, higherTimestamp);

    // Create resolver
    BulkUpdateAnomaliesResolver resolver =
        new BulkUpdateAnomaliesResolver(
            mockAssertionService, mockMonitorService, mockEntityClient, fixedTimeProvider);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    List<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent> results =
        resolver.get(mockEnv).get();

    // Verify results
    assertEquals(results.size(), 3);

    // Since higherTimestamp > FIXED_TIME, should use higherTimestamp
    assertEquals(results.get(0).getTimestampMillis(), higherTimestamp + 1);
    assertEquals(results.get(1).getTimestampMillis(), higherTimestamp + 2);
    assertEquals(results.get(2).getTimestampMillis(), higherTimestamp + 3);

    // Verify other properties
    for (com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent result : results) {
      assertEquals(
          result.getState(), com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);
      assertEquals(result.getSource().getSourceUrn(), TEST_ASSERTION_URN.toString());
    }

    // Verify batchIngestProposals was called
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(list -> list.size() == 3),
            Mockito.eq(false));
  }

  @Test
  public void testGetWithFallbackToCurrentTime() throws Exception {
    // Create mocks
    AssertionService mockAssertionService = initMockAssertionServiceWithMultipleEvents();
    MonitorService mockMonitorService = initMockMonitorService();
    EntityClient mockEntityClient = Mockito.mock(EntityClient.class);
    LongSupplier fixedTimeProvider = () -> FIXED_TIME;

    // Mock getTimeseriesAspectValues to throw exception, triggering fallback
    Mockito.when(
            mockEntityClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                Mockito.eq(TEST_MONITOR_URN.toString()),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                Mockito.isNull(),
                Mockito.isNull(),
                Mockito.eq(1),
                Mockito.isNull()))
        .thenThrow(new RemoteInvocationException("Failed to fetch"));

    // Create resolver
    BulkUpdateAnomaliesResolver resolver =
        new BulkUpdateAnomaliesResolver(
            mockAssertionService, mockMonitorService, mockEntityClient, fixedTimeProvider);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    List<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent> results =
        resolver.get(mockEnv).get();

    // Verify results
    assertEquals(results.size(), 3);

    // When fallback occurs, timestamps should increment from FIXED_TIME (the time provider)
    assertEquals(results.get(0).getTimestampMillis(), FIXED_TIME + 1);
    assertEquals(results.get(1).getTimestampMillis(), FIXED_TIME + 2);
    assertEquals(results.get(2).getTimestampMillis(), FIXED_TIME + 3);

    // Verify other properties
    for (com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent result : results) {
      assertEquals(
          result.getState(), com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);
      assertEquals(result.getSource().getSourceUrn(), TEST_ASSERTION_URN.toString());
    }

    // Verify batchIngestProposals was called
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.argThat(list -> list.size() == 3),
            Mockito.eq(false));
  }
}
