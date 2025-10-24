package com.linkedin.datahub.graphql.resolvers.monitor;

import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.BulkUpdateAnomaliesInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BulkUpdateAnomaliesResolver
    implements DataFetcher<
        CompletableFuture<List<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent>>> {

  private final AssertionService _assertionService;
  private final MonitorService _monitorService;
  private final EntityClient _entityClient;
  final LongSupplier _timeProvider;

  public BulkUpdateAnomaliesResolver(
      @Nonnull final AssertionService assertionService,
      @Nonnull final MonitorService monitorService,
      @Nonnull final EntityClient entityClient) {
    this(assertionService, monitorService, entityClient, System::currentTimeMillis);
  }

  public BulkUpdateAnomaliesResolver(
      @Nonnull final AssertionService assertionService,
      @Nonnull final MonitorService monitorService,
      @Nonnull final EntityClient entityClient,
      @Nonnull final LongSupplier timeProvider) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _assertionService = Objects.requireNonNull(assertionService, "assertionService is required");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient is required");
    _timeProvider = timeProvider;
  }

  @Override
  public CompletableFuture<List<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent>> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext queryContext = environment.getContext();
    final BulkUpdateAnomaliesInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), BulkUpdateAnomaliesInput.class);
    final Urn monitorUrn = Urn.createFromString(input.getMonitorUrn());
    final Urn assertionUrn = Urn.createFromString(input.getAssertionUrn());
    final long startTimeMillis = input.getStartTimeMillis();
    final long endTimeMillis = input.getEndTimeMillis();
    final AnomalyReviewState state = AnomalyReviewState.valueOf(input.getState().name());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // 1. Get the run events for this monitor in the time range
          final List<AssertionRunEvent> runEvents =
              _assertionService.getAssertionRunEvents(
                  queryContext.getOperationContext(), assertionUrn, startTimeMillis, endTimeMillis);

          if (runEvents.isEmpty()) {
            log.info(
                "No run events found for assertion {} in time range {} to {}",
                assertionUrn,
                startTimeMillis,
                endTimeMillis);
            return new ArrayList<>();
          }

          // 2. Get the latest anomaly event timestamp for this monitor
          // Since we are bulk updating anomaly events, we need to get the latest anomaly event
          // timestamp, and increment from there to avoid duplicate timestamps
          // We use the max of current time and the latest anomaly event timestamp to ensure
          // timestamps are always moving forward
          final long currentTimeMillis = _timeProvider.getAsLong();
          long latestAnomalyEventTimestampMillis;
          try {
            long fetchedLatestTimestamp =
                MonitorAnomalyEventUtils.getLatestAnomalyEventTimestamp(
                    queryContext.getOperationContext(), this._entityClient, monitorUrn);
            latestAnomalyEventTimestampMillis = Math.max(currentTimeMillis, fetchedLatestTimestamp);
            log.info(
                "Using max timestamp for monitor {}: current time = {}, fetched latest = {}, using = {}",
                monitorUrn,
                currentTimeMillis,
                fetchedLatestTimestamp,
                latestAnomalyEventTimestampMillis);
          } catch (RemoteInvocationException exception) {
            log.warn(
                "Failed to get latest anomaly event timestamp for monitor {}. Using current time as fallback. Error: {}",
                monitorUrn,
                exception.getMessage());
            latestAnomalyEventTimestampMillis = currentTimeMillis;
          }

          // 3. For each run event, construct the anomaly event
          final List<MonitorAnomalyEvent> anomalyEvents = new ArrayList<>();
          final List<MetadataChangeProposal> proposals = new ArrayList<>();

          for (AssertionRunEvent runEvent : runEvents) {
            latestAnomalyEventTimestampMillis++;
            final MonitorAnomalyEvent anomalyEvent =
                MonitorAnomalyEventUtils.buildMonitorAnomalyEvent(
                    assertionUrn, runEvent, state, latestAnomalyEventTimestampMillis);
            log.info(
                "Creating anomaly event for run event {} with timestamp {} and state {}",
                runEvent.getTimestampMillis(),
                latestAnomalyEventTimestampMillis,
                state);

            anomalyEvents.add(anomalyEvent);

            // Build MCP for each anomaly event
            proposals.add(
                AspectUtils.buildSynchronousMetadataChangeProposal(
                    monitorUrn, Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME, anomalyEvent));
          }

          // 4. Batch ingest the anomaly event MCPs
          try {
            this._entityClient.batchIngestProposals(
                queryContext.getOperationContext(), proposals, false);

            // 5. Best attempt to trigger retraining of the monitor
            tryTriggerMonitorRetraining(monitorUrn);

            // 6. Return mapped anomaly events
            return anomalyEvents.stream()
                .map(MonitorMapper::mapMonitorAnomalyEvent)
                .collect(Collectors.toList());
          } catch (RemoteInvocationException exception) {
            throw new DataHubGraphQLException(
                String.format(
                    "Failed to bulk update anomaly events for monitor %s in time range %s to %s. Error: %s",
                    monitorUrn, startTimeMillis, endTimeMillis, exception.getMessage()),
                DataHubGraphQLErrorCode.SERVER_ERROR);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void tryTriggerMonitorRetraining(@Nonnull final Urn monitorUrn) {
    try {
      this._monitorService.retrainAssertionMonitor(monitorUrn);
    } catch (Exception e) {
      log.warn(
          "Failed to trigger retraining for monitor {} after bulk updating anomaly feedback. Error: {}",
          monitorUrn,
          e.getMessage());
    }
  }
}
