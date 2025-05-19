package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceProperties;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent;
import com.linkedin.datahub.graphql.generated.ReportAnomalyInput;
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

public class ReportAnomalyResolver implements DataFetcher<CompletableFuture<MonitorAnomalyEvent>> {

  private final AssertionService _assertionService;
  private final MonitorService _monitorService;
  private final EntityClient _entityClient;
  final LongSupplier _timeProvider;

  public ReportAnomalyResolver(
      @Nonnull final AssertionService assertionService,
      @Nonnull final MonitorService monitorService,
      @Nonnull final EntityClient entityClient) {
    this(assertionService, monitorService, entityClient, System::currentTimeMillis);
  }

  public ReportAnomalyResolver(
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
    final Urn actorUrn = Urn.createFromString(queryContext.getActorUrn());
    final ReportAnomalyInput input =
        ResolverUtils.bindArgument(environment.getArgument("input"), ReportAnomalyInput.class);
    final Urn monitorUrn = Urn.createFromString(input.getMonitorUrn());
    final Urn assertionUrn = Urn.createFromString(input.getAssertionUrn());
    final long runEventTimestampMillis = input.getRunEventTimestampMillis();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // 1. ensure there is not already an anomaly event for this monitor
          final com.linkedin.anomaly.MonitorAnomalyEvent existingAnomalyEvent =
              _monitorService.getAnomalyEvent(
                  queryContext.getOperationContext(), monitorUrn, runEventTimestampMillis);
          if (existingAnomalyEvent != null) {
            throw new DataHubGraphQLException(
                String.format(
                    "Anomaly event already exists for monitor %s with timestamp %s",
                    monitorUrn, runEventTimestampMillis),
                DataHubGraphQLErrorCode.CONFLICT);
          }

          // 2. get the run event for this monitor
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

          // 3. create the anomaly event

          final AnomalySourceProperties anomalySourceProperties = new AnomalySourceProperties();
          if (runEvent.hasResult() && runEvent.getResult().hasMetric()) {
            anomalySourceProperties.setAssertionMetric(runEvent.getResult().getMetric());
          }

          final AnomalySource anomalySource = new AnomalySource();
          anomalySource.setSourceUrn(actorUrn);
          anomalySource.setType(AnomalySourceType.USER_FEEDBACK);
          anomalySource.setProperties(anomalySourceProperties);

          final TimeStamp now = new TimeStamp().setTime(_timeProvider.getAsLong());
          final com.linkedin.anomaly.MonitorAnomalyEvent anomalyEvent =
              new com.linkedin.anomaly.MonitorAnomalyEvent();
          anomalyEvent.setTimestampMillis(runEventTimestampMillis);
          anomalyEvent.setState(AnomalyReviewState.CONFIRMED);
          anomalyEvent.setSource(anomalySource);
          anomalyEvent.setCreated(now);
          anomalyEvent.setLastUpdated(now);

          // 4. ingest the anomaly event
          try {
            this._entityClient.ingestProposal(
                queryContext.getOperationContext(),
                AspectUtils.buildMetadataChangeProposal(
                    monitorUrn, Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME, anomalyEvent));
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
}
