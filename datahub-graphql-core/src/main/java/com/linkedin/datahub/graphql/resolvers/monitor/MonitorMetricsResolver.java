package com.linkedin.datahub.graphql.resolvers.monitor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AssertionMetric;
import com.linkedin.datahub.graphql.generated.ListMonitorMetricsInput;
import com.linkedin.datahub.graphql.generated.ListMonitorMetricsResult;
import com.linkedin.datahub.graphql.generated.MonitorMetric;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metric.DataHubMetricCubeEvent;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
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

  /** Fetches the anomaly events for a given monitor within the time range. */
  @Nonnull
  private List<MonitorAnomalyEvent> fetchAnomalyEvents(
      @Nonnull QueryContext context,
      @Nonnull String monitorUrn,
      @Nonnull Long startTimeMillis,
      @Nonnull Long endTimeMillis)
      throws RemoteInvocationException {
    return MonitorAnomalyEventUtils.fetchAnomalyEvents(
        context.getOperationContext(), _client, monitorUrn, startTimeMillis, endTimeMillis);
  }

  /** Builds a map from source event timestamp to anomaly event. */
  @Nonnull
  private Map<Long, MonitorAnomalyEvent> buildAnomalyEventMap(
      @Nonnull List<MonitorAnomalyEvent> anomalyEvents) {
    return MonitorAnomalyEventUtils.buildAnomalyEventMapByMetric(anomalyEvents);
  }
}
