package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AssertionMetric;
import com.linkedin.datahub.graphql.generated.ListMonitorMetricsInput;
import com.linkedin.datahub.graphql.generated.ListMonitorMetricsResult;
import com.linkedin.datahub.graphql.generated.MonitorMetric;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metric.DataHubMetricCubeEvent;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** GraphQL Resolver used for fetching Monitor Metrics. */
@Slf4j
public class MonitorMetricsResolver
    implements DataFetcher<CompletableFuture<ListMonitorMetricsResult>> {

  private final EntityClient _client;

  public MonitorMetricsResolver(final EntityClient client) {
    _client = client;
  }

  @Override
  public CompletableFuture<ListMonitorMetricsResult> get(DataFetchingEnvironment environment) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final ListMonitorMetricsInput input =
              bindArgument(environment.getArgument("input"), ListMonitorMetricsInput.class);

          try {
            // Step 1: Construct the metric cube URN from the monitor URN
            final String metricCubeUrn = buildMetricCubeUrn(input.getMonitorUrn());

            // Step 2: Fetch metric cube events from GMS
            final List<MonitorMetric> metrics =
                fetchMetricCubeEvents(
                    context, metricCubeUrn, input.getStartTimeMillis(), input.getEndTimeMillis());

            // Step 3: Fetch anomaly events from GMS and hydrate metrics
            final List<MonitorAnomalyEvent> anomalyEvents =
                fetchAnomalyEvents(
                    context,
                    input.getMonitorUrn(),
                    input.getStartTimeMillis(),
                    input.getEndTimeMillis());

            // Step 4: Map anomaly events to metrics by timestamp
            final Map<Long, MonitorAnomalyEvent> anomalyEventMap =
                buildAnomalyEventMap(anomalyEvents);

            // Hydrate metrics with matching anomaly events
            metrics.forEach(
                metric -> {
                  final MonitorAnomalyEvent matchingAnomalyEvent =
                      anomalyEventMap.get(metric.getAssertionMetric().getTimestampMillis());
                  if (matchingAnomalyEvent != null) {
                    metric.setAnomalyEvent(
                        MonitorMapper.mapMonitorAnomalyEvent(matchingAnomalyEvent));
                  }
                });

            // Step 5: Package and return response
            final ListMonitorMetricsResult result = new ListMonitorMetricsResult();
            result.setMetrics(metrics);
            return result;
          } catch (RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve Monitor Metrics from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Constructs the metric cube URN from the monitor URN using base64 encoding. The format is:
   * urn:li:dataHubMetricCube:{base64EncodedMonitorUrn}
   */
  @Nonnull
  private String buildMetricCubeUrn(@Nonnull String monitorUrn) {
    final String encodedMonitorUrn = Base64.getUrlEncoder().encodeToString(monitorUrn.getBytes());
    return String.format("urn:li:dataHubMetricCube:%s", encodedMonitorUrn);
  }

  /** Fetches metric cube events for the given metric cube URN and time range. */
  @Nonnull
  private List<MonitorMetric> fetchMetricCubeEvents(
      @Nonnull QueryContext context,
      @Nonnull String metricCubeUrn,
      @Nonnull Long startTimeMillis,
      @Nonnull Long endTimeMillis)
      throws RemoteInvocationException {

    final List<EnvelopedAspect> aspects =
        _client.getTimeseriesAspectValues(
            context.getOperationContext(),
            metricCubeUrn,
            METRIC_CUBE_ENTITY_NAME,
            METRIC_CUBE_EVENT_ASPECT_NAME,
            startTimeMillis,
            endTimeMillis,
            null, // no limit
            null); // no filter

    return aspects.stream().map(this::mapToMonitorMetric).collect(Collectors.toList());
  }

  /** Maps an EnvelopedAspect containing a DataHubMetricCubeEvent to a MonitorMetric. */
  @Nonnull
  private MonitorMetric mapToMonitorMetric(@Nonnull EnvelopedAspect aspect) {
    final DataHubMetricCubeEvent metricCubeEvent =
        GenericRecordUtils.deserializeAspect(
            aspect.getAspect().getValue(),
            aspect.getAspect().getContentType(),
            DataHubMetricCubeEvent.class);

    final AssertionMetric assertionMetric = new AssertionMetric();
    assertionMetric.setTimestampMillis(metricCubeEvent.getTimestampMillis());
    assertionMetric.setValue(metricCubeEvent.getMeasure().floatValue());

    final MonitorMetric monitorMetric = new MonitorMetric();
    monitorMetric.setAssertionMetric(assertionMetric);

    return monitorMetric;
  }

  /**
   * Fetches the anomaly events for a given monitor within the time range. This is copied from
   * AssertionRunEventResolver with minor modifications.
   */
  @Nonnull
  private List<MonitorAnomalyEvent> fetchAnomalyEvents(
      @Nonnull QueryContext context,
      @Nonnull String monitorUrn,
      @Nonnull Long startTimeMillis,
      @Nonnull Long endTimeMillis)
      throws RemoteInvocationException {

    final List<EnvelopedAspect> aspects =
        _client.getTimeseriesAspectValues(
            context.getOperationContext(),
            monitorUrn,
            MONITOR_ENTITY_NAME,
            MONITOR_ANOMALY_EVENT_ASPECT_NAME,
            null,
            null,
            null, // no limit to capture all anomaly events in the time range
            startTimeMillis != null || endTimeMillis != null
                ? buildAnomalyFeedbackEventsFilter(startTimeMillis, endTimeMillis)
                : null);

    return aspects.stream()
        .map(
            aspect ->
                GenericRecordUtils.deserializeAspect(
                    aspect.getAspect().getValue(),
                    aspect.getAspect().getContentType(),
                    com.linkedin.anomaly.MonitorAnomalyEvent.class))
        .collect(Collectors.toList());
  }

  @Nonnull
  protected static Filter buildAnomalyFeedbackEventsFilter(
      @Nullable Long maybeStartTimeMillis, @Nullable Long maybeEndTimeMillis) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            ImmutableList.of(
                                new Criterion()
                                    .setField("sourceEventTimestampMillis")
                                    .setCondition(Condition.BETWEEN)
                                    .setValues(
                                        new StringArray(
                                            List.of(
                                                maybeStartTimeMillis != null
                                                    ? maybeStartTimeMillis.toString()
                                                    : "0",
                                                maybeEndTimeMillis != null
                                                    ? maybeEndTimeMillis.toString()
                                                    : String.valueOf(Long.MAX_VALUE)))))))));
  }

  /**
   * Builds a map from source event timestamp to anomaly event. This is copied from
   * AssertionRunEventResolver.
   */
  @Nonnull
  private static Map<Long, MonitorAnomalyEvent> buildAnomalyEventMap(
      @Nonnull List<MonitorAnomalyEvent> anomalyEvents) {
    final Map<Long, MonitorAnomalyEvent> anomalyEventMap = new HashMap<>();

    for (MonitorAnomalyEvent anomalyEvent : anomalyEvents) {
      if (anomalyEvent.getSource().hasProperties()
          && anomalyEvent.getSource().getProperties().hasAssertionMetric()
          && anomalyEvent.getSource().getProperties().getAssertionMetric().getTimestampMs()
              == null) {
        continue;
      }
      final MonitorAnomalyEvent maybeExistingEvent =
          anomalyEventMap.get(
              anomalyEvent.getSource().getProperties().getAssertionMetric().getTimestampMs());
      // If an anomaly event already exists for this timestamp, skip if the new one is NOT more
      // recent
      if (maybeExistingEvent != null
          && anomalyEvent.getSource().getProperties().getAssertionMetric().getTimestampMs()
              <= maybeExistingEvent
                  .getSource()
                  .getProperties()
                  .getAssertionMetric()
                  .getTimestampMs()) {
        continue;
      }
      // Use the timestamp as the key to map anomaly events to metrics
      anomalyEventMap.put(
          anomalyEvent.getSource().getProperties().getAssertionMetric().getTimestampMs(),
          anomalyEvent);
    }

    return anomalyEventMap;
  }
}
