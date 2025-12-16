package com.linkedin.datahub.graphql.resolvers.monitor;

import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ListMonitorAnomaliesFilterInput;
import com.linkedin.datahub.graphql.generated.ListMonitorAnomaliesInput;
import com.linkedin.datahub.graphql.generated.ListMonitorAnomaliesResult;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** GraphQL Resolver used for fetching Monitor Anomaly Events. */
@Slf4j
public class ListMonitorAnomaliesResolver
    implements DataFetcher<CompletableFuture<ListMonitorAnomaliesResult>> {

  private final EntityClient _client;

  public ListMonitorAnomaliesResolver(final EntityClient client) {
    _client = client;
  }

  @Override
  public CompletableFuture<ListMonitorAnomaliesResult> get(DataFetchingEnvironment environment) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final ListMonitorAnomaliesInput input =
              ResolverUtils.bindArgument(
                  environment.getArgument("input"), ListMonitorAnomaliesInput.class);

          try {
            // Fetch anomaly events from GMS
            final List<com.linkedin.anomaly.MonitorAnomalyEvent> anomalyEvents =
                MonitorAnomalyEventUtils.fetchAnomalyEvents(
                    context.getOperationContext(),
                    _client,
                    input.getMonitorUrn(),
                    input.getStartTimeMillis(),
                    input.getEndTimeMillis());

            List<com.linkedin.anomaly.MonitorAnomalyEvent> filteredAnomalyEvents = anomalyEvents;
            final ListMonitorAnomaliesFilterInput filter = input.getFilter();
            if (filter != null) {

              // If requested, keep only the latest anomaly event for each source event timestamp.
              if (Boolean.TRUE.equals(filter.getLatestBySourceEventOnly())) {
                filteredAnomalyEvents =
                    new ArrayList<>(
                        MonitorAnomalyEventUtils.buildAnomalyEventMapBySourceEvent(
                                filteredAnomalyEvents)
                            .values());
                filteredAnomalyEvents.sort(
                    Comparator.comparingLong(
                            (com.linkedin.anomaly.MonitorAnomalyEvent event) ->
                                event.getTimestampMillis())
                        .reversed()); // keep descending for consistent output ordering
              }

              // Optionally filter down to a set of allowed states. Applied after dedupe so the
              // latest state "wins" for each source event.
              if (filter.getStates() != null && !filter.getStates().isEmpty()) {
                final Set<AnomalyReviewState> allowedStates =
                    filter.getStates().stream()
                        .map(state -> AnomalyReviewState.valueOf(state.name()))
                        .collect(Collectors.toSet());
                filteredAnomalyEvents =
                    filteredAnomalyEvents.stream()
                        .filter(
                            event -> event.hasState() && allowedStates.contains(event.getState()))
                        .toList();
              }
            }

            // Map to GraphQL types
            final ListMonitorAnomaliesResult result = new ListMonitorAnomaliesResult();
            result.setAnomalies(
                filteredAnomalyEvents.stream()
                    .map(MonitorMapper::mapMonitorAnomalyEvent)
                    .collect(Collectors.toList()));

            return result;
          } catch (RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve Monitor Anomalies from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
