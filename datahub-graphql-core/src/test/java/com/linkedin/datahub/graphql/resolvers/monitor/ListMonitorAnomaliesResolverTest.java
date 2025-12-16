package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.metadata.Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.MONITOR_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.common.TimeStamp;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListMonitorAnomaliesFilterInput;
import com.linkedin.datahub.graphql.generated.ListMonitorAnomaliesInput;
import com.linkedin.datahub.graphql.generated.ListMonitorAnomaliesResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.SystemMetadata;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListMonitorAnomaliesResolverTest {

  private static final String TEST_MONITOR_URN = "urn:li:monitor:test-monitor";
  private static final long START_TIME = 0L;
  private static final long END_TIME = 1000L;

  @Test
  public void testGet_noFilter_returnsAllEvents() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final List<EnvelopedAspect> aspects =
        ImmutableList.of(
            makeAnomalyAspect(10L, 300L, AnomalyReviewState.CONFIRMED),
            makeAnomalyAspect(10L, 200L, AnomalyReviewState.REJECTED),
            makeAnomalyAspect(20L, 100L, null));

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                eq(TEST_MONITOR_URN),
                eq(MONITOR_ENTITY_NAME),
                eq(MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                isNull(),
                isNull(),
                isNull(),
                any(),
                any(SortCriterion.class)))
        .thenReturn(aspects);

    final ListMonitorAnomaliesResolver resolver = new ListMonitorAnomaliesResolver(mockClient);

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final ListMonitorAnomaliesInput input = new ListMonitorAnomaliesInput();
    input.setMonitorUrn(TEST_MONITOR_URN);
    input.setStartTimeMillis(START_TIME);
    input.setEndTimeMillis(END_TIME);
    // no filter

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    final CompletableFuture<ListMonitorAnomaliesResult> resultFuture = resolver.get(mockEnv);
    final ListMonitorAnomaliesResult result = resultFuture.get();

    assertNotNull(result);
    assertEquals(result.getAnomalies().size(), 3);
    assertEquals(result.getAnomalies().get(0).getSource().getSourceEventTimestampMillis(), 10L);
    assertEquals(result.getAnomalies().get(0).getTimestampMillis(), 300L);
    assertEquals(result.getAnomalies().get(1).getSource().getSourceEventTimestampMillis(), 10L);
    assertEquals(result.getAnomalies().get(1).getTimestampMillis(), 200L);
    assertEquals(result.getAnomalies().get(2).getSource().getSourceEventTimestampMillis(), 20L);
    assertEquals(result.getAnomalies().get(2).getTimestampMillis(), 100L);
    assertNull(result.getAnomalies().get(2).getState());
  }

  @Test
  public void testGet_latestBySourceEventOnly_dedupesBySourceEventTimestamp() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final List<EnvelopedAspect> aspects =
        ImmutableList.of(
            // two events for sourceEventTimestamp=10, latest timestampMillis=300 should win
            makeAnomalyAspect(10L, 300L, AnomalyReviewState.CONFIRMED),
            makeAnomalyAspect(10L, 200L, AnomalyReviewState.REJECTED),
            makeAnomalyAspect(20L, 100L, AnomalyReviewState.CONFIRMED));

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                eq(TEST_MONITOR_URN),
                eq(MONITOR_ENTITY_NAME),
                eq(MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                isNull(),
                isNull(),
                isNull(),
                any(),
                any(SortCriterion.class)))
        .thenReturn(aspects);

    final ListMonitorAnomaliesResolver resolver = new ListMonitorAnomaliesResolver(mockClient);

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final ListMonitorAnomaliesFilterInput filter = new ListMonitorAnomaliesFilterInput();
    filter.setLatestBySourceEventOnly(true);

    final ListMonitorAnomaliesInput input = new ListMonitorAnomaliesInput();
    input.setMonitorUrn(TEST_MONITOR_URN);
    input.setStartTimeMillis(START_TIME);
    input.setEndTimeMillis(END_TIME);
    input.setFilter(filter);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    final ListMonitorAnomaliesResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getAnomalies().size(), 2);
    // Output is sorted by timestampMillis descending after dedupe.
    assertEquals(result.getAnomalies().get(0).getSource().getSourceEventTimestampMillis(), 10L);
    assertEquals(result.getAnomalies().get(0).getTimestampMillis(), 300L);
    assertEquals(result.getAnomalies().get(1).getSource().getSourceEventTimestampMillis(), 20L);
    assertEquals(result.getAnomalies().get(1).getTimestampMillis(), 100L);
  }

  @Test
  public void testGet_states_filtersByProvidedStates() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final List<EnvelopedAspect> aspects =
        ImmutableList.of(
            makeAnomalyAspect(10L, 300L, AnomalyReviewState.CONFIRMED),
            makeAnomalyAspect(11L, 200L, AnomalyReviewState.REJECTED),
            makeAnomalyAspect(12L, 100L, null));

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                eq(TEST_MONITOR_URN),
                eq(MONITOR_ENTITY_NAME),
                eq(MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                isNull(),
                isNull(),
                isNull(),
                any(),
                any(SortCriterion.class)))
        .thenReturn(aspects);

    final ListMonitorAnomaliesResolver resolver = new ListMonitorAnomaliesResolver(mockClient);

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final ListMonitorAnomaliesFilterInput filter = new ListMonitorAnomaliesFilterInput();
    filter.setStates(
        ImmutableList.of(com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED));

    final ListMonitorAnomaliesInput input = new ListMonitorAnomaliesInput();
    input.setMonitorUrn(TEST_MONITOR_URN);
    input.setStartTimeMillis(START_TIME);
    input.setEndTimeMillis(END_TIME);
    input.setFilter(filter);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    final ListMonitorAnomaliesResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getAnomalies().size(), 1);
    assertEquals(result.getAnomalies().get(0).getSource().getSourceEventTimestampMillis(), 10L);
    assertEquals(result.getAnomalies().get(0).getTimestampMillis(), 300L);
    assertEquals(
        result.getAnomalies().get(0).getState(),
        com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);
  }

  @Test
  public void testGet_latestBySourceEventOnlyThenStates_latestStateWins() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final List<EnvelopedAspect> aspects =
        ImmutableList.of(
            // For sourceEventTimestamp=10, latest is REJECTED -> should be filtered out when
            // requesting states=[CONFIRMED], even though an older CONFIRMED exists.
            makeAnomalyAspect(10L, 300L, AnomalyReviewState.REJECTED),
            makeAnomalyAspect(10L, 200L, AnomalyReviewState.CONFIRMED),
            makeAnomalyAspect(20L, 100L, AnomalyReviewState.CONFIRMED));

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                eq(TEST_MONITOR_URN),
                eq(MONITOR_ENTITY_NAME),
                eq(MONITOR_ANOMALY_EVENT_ASPECT_NAME),
                isNull(),
                isNull(),
                isNull(),
                any(),
                any(SortCriterion.class)))
        .thenReturn(aspects);

    final ListMonitorAnomaliesResolver resolver = new ListMonitorAnomaliesResolver(mockClient);

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final ListMonitorAnomaliesFilterInput filter = new ListMonitorAnomaliesFilterInput();
    filter.setLatestBySourceEventOnly(true);
    filter.setStates(
        ImmutableList.of(com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED));

    final ListMonitorAnomaliesInput input = new ListMonitorAnomaliesInput();
    input.setMonitorUrn(TEST_MONITOR_URN);
    input.setStartTimeMillis(START_TIME);
    input.setEndTimeMillis(END_TIME);
    input.setFilter(filter);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    final ListMonitorAnomaliesResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getAnomalies().size(), 1);
    assertEquals(result.getAnomalies().get(0).getSource().getSourceEventTimestampMillis(), 20L);
    assertEquals(result.getAnomalies().get(0).getTimestampMillis(), 100L);
    assertEquals(
        result.getAnomalies().get(0).getState(),
        com.linkedin.datahub.graphql.generated.AnomalyReviewState.CONFIRMED);
  }

  private static EnvelopedAspect makeAnomalyAspect(
      final long sourceEventTimestampMillis,
      final long anomalyEventTimestampMillis,
      final AnomalyReviewState maybeState) {
    final MonitorAnomalyEvent event = new MonitorAnomalyEvent();
    event.setTimestampMillis(anomalyEventTimestampMillis);

    if (maybeState != null) {
      event.setState(maybeState);
    }

    final TimeStamp now = new TimeStamp().setTime(anomalyEventTimestampMillis);
    event.setCreated(now);
    event.setLastUpdated(now);

    final AnomalySource source = new AnomalySource();
    source.setSourceEventTimestampMillis(sourceEventTimestampMillis);
    source.setType(AnomalySourceType.USER_FEEDBACK);
    event.setSource(source);

    return new EnvelopedAspect()
        .setAspect(GenericRecordUtils.serializeAspect(event))
        .setSystemMetadata(new SystemMetadata());
  }

  private static QueryContext getMockAllowContext() {
    final QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    return mockContext;
  }
}
