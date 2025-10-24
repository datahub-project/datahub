package com.linkedin.datahub.graphql.resolvers.monitor;

import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent;
import com.linkedin.datahub.graphql.generated.ReportAnomalyFeedbackInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReportAnomalyFeedbackResolver
    implements DataFetcher<CompletableFuture<MonitorAnomalyEvent>> {

  private final AssertionService _assertionService;
  private final MonitorService _monitorService;
  private final EntityClient _entityClient;
  final LongSupplier _timeProvider;

  public ReportAnomalyFeedbackResolver(
      @Nonnull final AssertionService assertionService,
      @Nonnull final MonitorService monitorService,
      @Nonnull final EntityClient entityClient) {
    this(assertionService, monitorService, entityClient, System::currentTimeMillis);
  }

  public ReportAnomalyFeedbackResolver(
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
  public CompletableFuture<MonitorAnomalyEvent> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext queryContext = environment.getContext();
    final ReportAnomalyFeedbackInput input =
        ResolverUtils.bindArgument(
            environment.getArgument("input"), ReportAnomalyFeedbackInput.class);
    final Urn monitorUrn = Urn.createFromString(input.getMonitorUrn());
    final Urn assertionUrn = Urn.createFromString(input.getAssertionUrn());
    final long runEventTimestampMillis = input.getRunEventTimestampMillis();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // 1. get the run event for this monitor
          final AssertionRunEvent runEvent =
              _assertionService.getAssertionRunEvent(
                  queryContext.getOperationContext(), assertionUrn, runEventTimestampMillis);
          if (runEvent == null) {
            throw new DataHubGraphQLException(
                String.format(
                    "Run event not found for monitor %s with timestamp %s",
                    monitorUrn, runEventTimestampMillis),
                DataHubGraphQLErrorCode.NOT_FOUND);
          }

          // 2. get the latest anomaly event timestamp
          // We use the max of current time and the latest anomaly event timestamp to ensure
          // timestamps are always moving forward
          final long currentTimeMillis = _timeProvider.getAsLong();
          long anomalyEventTimestampMillis;
          try {
            final long latestAnomalyEventTimestampMillis =
                MonitorAnomalyEventUtils.getLatestAnomalyEventTimestamp(
                    queryContext.getOperationContext(), this._entityClient, monitorUrn);
            anomalyEventTimestampMillis =
                Math.max(currentTimeMillis, latestAnomalyEventTimestampMillis);
          } catch (Exception e) {
            log.warn(
                "Failed to get latest anomaly event timestamp for monitor {}. Using current time as fallback. Error: {}",
                monitorUrn,
                e.getMessage());
            anomalyEventTimestampMillis = currentTimeMillis;
          }

          // 3. create the anomaly event
          final com.linkedin.anomaly.MonitorAnomalyEvent anomalyEvent =
              MonitorAnomalyEventUtils.buildMonitorAnomalyEvent(
                  assertionUrn,
                  runEvent,
                  AnomalyReviewState.valueOf(input.getState().name()),
                  anomalyEventTimestampMillis);

          // 4. ingest the anomaly event
          try {
            this._entityClient.ingestProposal(
                queryContext.getOperationContext(),
                AspectUtils.buildSynchronousMetadataChangeProposal(
                    monitorUrn, Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME, anomalyEvent),
                false);
            // best attempt to trigger retraining of the monitor
            tryTriggerMonitorRetraining(monitorUrn);
            return MonitorMapper.mapMonitorAnomalyEvent(anomalyEvent);
          } catch (RemoteInvocationException exception) {
            throw new DataHubGraphQLException(
                String.format(
                    "Failed to report anomaly event for monitor %s with timestamp %s. Error: %s",
                    monitorUrn, runEventTimestampMillis, exception.getMessage()),
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
          "Failed to trigger retraining for monitor {} after reporting anomaly feedback. Error: {}",
          monitorUrn,
          e.getMessage());
    }
  }
}
