package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.metadata.Constants.METRIC_CUBE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.METRIC_CUBE_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.MONITOR_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceProperties;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.assertion.AssertionMetric;
import com.linkedin.common.TimeStamp;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListMonitorMetricsInput;
import com.linkedin.datahub.graphql.generated.ListMonitorMetricsResult;
import com.linkedin.datahub.graphql.generated.MonitorMetric;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metric.DataHubMetricCubeEvent;
import com.linkedin.mxe.SystemMetadata;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MonitorMetricsResolverTest {

  private static final String TEST_MONITOR_URN = "urn:li:monitor:test-monitor";
  private static final String EXPECTED_METRIC_CUBE_URN =
      "urn:li:dataHubMetricCube:"
          + Base64.getUrlEncoder().encodeToString(TEST_MONITOR_URN.getBytes());
  private static final Long TEST_START_TIME = 1000L;
  private static final Long TEST_END_TIME = 2000L;
  private static final Long TEST_TIMESTAMP = 1500L;
  private static final Long TEST_RUN_TIMESTAMP = 1501L;
  private static final Double TEST_METRIC_VALUE = 42.5;

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Create mock DataHubMetricCubeEvent
    final DataHubMetricCubeEvent metricCubeEvent =
        new DataHubMetricCubeEvent()
            .setTimestampMillis(TEST_TIMESTAMP)
            .setMeasure(TEST_METRIC_VALUE)
            .setReportedTimeMillis(TEST_TIMESTAMP);

    // Create mock MonitorAnomalyEvent
    final MonitorAnomalyEvent anomalyEvent =
        new MonitorAnomalyEvent()
            .setTimestampMillis(1600L)
            .setLastUpdated(new TimeStamp().setTime(0L))
            .setCreated(new TimeStamp().setTime(0L))
            .setSource(
                new AnomalySource()
                    .setSourceEventTimestampMillis(TEST_RUN_TIMESTAMP)
                    .setType(AnomalySourceType.USER_FEEDBACK)
                    .setProperties(
                        new AnomalySourceProperties()
                            .setAssertionMetric(
                                new AssertionMetric()
                                    .setTimestampMs(TEST_TIMESTAMP)
                                    .setValue(TEST_METRIC_VALUE.floatValue()))));

    // Create EnvelopedAspects (following pattern from AssertionRunEventResolverTest)
    final EnvelopedAspect mockMetricAspect =
        new EnvelopedAspect()
            .setAspect(GenericRecordUtils.serializeAspect(metricCubeEvent))
            .setSystemMetadata(new SystemMetadata());

    final EnvelopedAspect mockAnomalyAspect =
        new EnvelopedAspect()
            .setAspect(GenericRecordUtils.serializeAspect(anomalyEvent))
            .setSystemMetadata(new SystemMetadata());

    // Mock EntityClient calls
    // Mock metric cube events
    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                eq(EXPECTED_METRIC_CUBE_URN),
                eq("dataHubMetricCube"),
                eq("dataHubMetricCubeEvent"),
                eq(TEST_START_TIME),
                eq(TEST_END_TIME),
                eq(null),
                eq(null)))
        .thenReturn(ImmutableList.of(mockMetricAspect));

    // Mock anomaly events
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
        .thenReturn(ImmutableList.of(mockAnomalyAspect));

    // Create resolver
    MonitorMetricsResolver resolver = new MonitorMetricsResolver(mockClient);

    // Create mock environment
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Create input
    ListMonitorMetricsInput input = new ListMonitorMetricsInput();
    input.setMonitorUrn(TEST_MONITOR_URN);
    input.setStartTimeMillis(TEST_START_TIME);
    input.setEndTimeMillis(TEST_END_TIME);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    // Execute resolver
    CompletableFuture<ListMonitorMetricsResult> resultFuture = resolver.get(mockEnv);
    ListMonitorMetricsResult result = resultFuture.get();

    // Verify results
    assertNotNull(result);
    assertNotNull(result.getMetrics());
    assertEquals(result.getMetrics().size(), 1);

    MonitorMetric metric = result.getMetrics().get(0);
    assertNotNull(metric.getAssertionMetric());
    assertEquals(metric.getAssertionMetric().getTimestampMillis(), TEST_TIMESTAMP);
    assertEquals(metric.getAssertionMetric().getValue(), TEST_METRIC_VALUE.floatValue());

    // Verify anomaly event is hydrated
    assertNotNull(metric.getAnomalyEvent());
    assertEquals(metric.getAnomalyEvent().getTimestampMillis(), anomalyEvent.getTimestampMillis());
  }

  @Test
  public void testGetSuccessNoAnomalyEvents() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Create mock DataHubMetricCubeEvent
    final DataHubMetricCubeEvent metricCubeEvent =
        new DataHubMetricCubeEvent()
            .setTimestampMillis(TEST_TIMESTAMP)
            .setMeasure(TEST_METRIC_VALUE)
            .setReportedTimeMillis(TEST_TIMESTAMP);

    // Create EnvelopedAspect (following pattern from AssertionRunEventResolverTest)
    final EnvelopedAspect mockMetricAspect =
        new EnvelopedAspect()
            .setAspect(GenericRecordUtils.serializeAspect(metricCubeEvent))
            .setSystemMetadata(new SystemMetadata());

    // Mock EntityClient calls
    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(OperationContext.class),
                eq(EXPECTED_METRIC_CUBE_URN),
                eq(METRIC_CUBE_ENTITY_NAME),
                eq(METRIC_CUBE_EVENT_ASPECT_NAME),
                eq(TEST_START_TIME),
                eq(TEST_END_TIME),
                eq(null),
                eq(null)))
        .thenReturn(ImmutableList.of(mockMetricAspect));

    // Mock no anomaly events
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
        .thenReturn(ImmutableList.of());

    // Create resolver
    MonitorMetricsResolver resolver = new MonitorMetricsResolver(mockClient);

    // Create mock environment
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Create input
    ListMonitorMetricsInput input = new ListMonitorMetricsInput();
    input.setMonitorUrn(TEST_MONITOR_URN);
    input.setStartTimeMillis(TEST_START_TIME);
    input.setEndTimeMillis(TEST_END_TIME);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    // Execute resolver
    CompletableFuture<ListMonitorMetricsResult> resultFuture = resolver.get(mockEnv);
    ListMonitorMetricsResult result = resultFuture.get();

    // Verify results
    assertNotNull(result);
    assertNotNull(result.getMetrics());
    assertEquals(result.getMetrics().size(), 1);

    MonitorMetric metric = result.getMetrics().get(0);
    assertNotNull(metric.getAssertionMetric());
    assertEquals(metric.getAssertionMetric().getTimestampMillis(), TEST_TIMESTAMP);
    assertEquals(metric.getAssertionMetric().getValue(), TEST_METRIC_VALUE.floatValue());

    // Verify no anomaly event
    assertNull(metric.getAnomalyEvent());
  }

  @Test
  public void testBuildMetricCubeUrn() {
    MonitorMetricsResolver resolver = new MonitorMetricsResolver(Mockito.mock(EntityClient.class));
    String monitorUrn = "urn:li:monitor:test-123";

    // Use reflection to access private method for testing
    try {
      java.lang.reflect.Method method =
          MonitorMetricsResolver.class.getDeclaredMethod("buildMetricCubeUrn", String.class);
      method.setAccessible(true);
      String result = (String) method.invoke(resolver, monitorUrn);

      String expectedEncoded = Base64.getUrlEncoder().encodeToString(monitorUrn.getBytes());
      String expected = "urn:li:dataHubMetricCube:" + expectedEncoded;

      assertEquals(result, expected);
    } catch (Exception e) {
      fail("Failed to test buildMetricCubeUrn: " + e.getMessage());
    }
  }

  @Test
  public void testBuildAnomalyFeedbackEventsFilter() {
    Long startTime = 1000L;
    Long endTime = 2000L;

    // Test with both start and end time
    var filter = MonitorAnomalyEventUtils.buildAnomalyFeedbackEventsFilter(startTime, endTime);
    assertNotNull(filter);
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 1);

    var conjunctiveCriterion = filter.getOr().get(0);
    assertEquals(conjunctiveCriterion.getAnd().size(), 1);

    var criterion = conjunctiveCriterion.getAnd().get(0);
    assertEquals(criterion.getField(), "sourceEventTimestampMillis");
    assertEquals(criterion.getCondition().name(), "BETWEEN");
    assertEquals(criterion.getValues().size(), 2);
    assertEquals(criterion.getValues().get(0), startTime.toString());
    assertEquals(criterion.getValues().get(1), endTime.toString());

    // Test with null values
    var filterWithNulls = MonitorAnomalyEventUtils.buildAnomalyFeedbackEventsFilter(null, null);
    assertNotNull(filterWithNulls);
    var criterionWithNulls = filterWithNulls.getOr().get(0).getAnd().get(0);
    assertEquals(criterionWithNulls.getValues().get(0), "0");
    assertEquals(criterionWithNulls.getValues().get(1), String.valueOf(Long.MAX_VALUE));
  }

  private static QueryContext getMockAllowContext() {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    return mockContext;
  }
}
