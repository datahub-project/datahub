package com.linkedin.datahub.graphql.resolvers.monitor;

import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AnomalyReviewState;
import com.linkedin.datahub.graphql.generated.UpdateAnomalyInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.service.MonitorService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;

public class UpdateAnomalyResolver
    implements DataFetcher<
        CompletableFuture<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent>> {

  private final EntityClient _entityClient;
  private final MonitorService _monitorService;
  final LongSupplier _timeProvider;

  public UpdateAnomalyResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final MonitorService monitorService) {
    this(entityClient, monitorService, System::currentTimeMillis);
  }

  public UpdateAnomalyResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final MonitorService monitorService,
      @Nonnull final LongSupplier timeProvider) {
    _monitorService = Objects.requireNonNull(monitorService, "monitorService is required");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient is required");
    _timeProvider = timeProvider;
  }

  @Override
  public CompletableFuture<com.linkedin.datahub.graphql.generated.MonitorAnomalyEvent> get(
      final DataFetchingEnvironment environment) throws Exception {
    final QueryContext queryContext = environment.getContext();

    final UpdateAnomalyInput input =
        ResolverUtils.bindArgument(environment.getArgument("input"), UpdateAnomalyInput.class);

    final Urn monitorUrn = UrnUtils.getUrn(input.getMonitorUrn());
    final long anomalyTimestamp = input.getAnomalyTimestampMillis();
    final AnomalyReviewState reviewState = input.getState();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final MonitorAnomalyEvent gmsEvent =
                this._monitorService.getAnomalyEvent(
                    queryContext.getOperationContext(), monitorUrn, anomalyTimestamp);
            if (gmsEvent == null) {
              throw new DataHubGraphQLException(
                  String.format("Anomaly event with timestamp %s not found", anomalyTimestamp),
                  DataHubGraphQLErrorCode.NOT_FOUND);
            }
            gmsEvent.setState(com.linkedin.anomaly.AnomalyReviewState.valueOf(reviewState.name()));

            this._entityClient.ingestProposal(
                queryContext.getOperationContext(),
                AspectUtils.buildMetadataChangeProposal(
                    monitorUrn, Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME, gmsEvent));

            return MonitorMapper.mapMonitorAnomalyEvent(gmsEvent);
          } catch (final Exception e) {
            throw new DataHubGraphQLException(
                String.format("Failed to update anomaly event. Error: %s", e.getMessage()),
                DataHubGraphQLErrorCode.SERVER_ERROR);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
