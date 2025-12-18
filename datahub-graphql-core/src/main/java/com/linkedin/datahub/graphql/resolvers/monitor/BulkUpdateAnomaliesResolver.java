package com.linkedin.datahub.graphql.resolvers.monitor;

import com.google.common.collect.ImmutableList;
import com.linkedin.anomaly.AnomalyReviewState;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.Operation;
import com.linkedin.common.OperationType;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.BulkUpdateAnomaliesInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.monitor.MonitorMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
          // 1. Check if this is a freshness assertion
          final AssertionInfo assertionInfo =
              _assertionService.getAssertionInfo(queryContext.getOperationContext(), assertionUrn);
          final boolean isFreshnessAssertion =
              assertionInfo != null && assertionInfo.getType() == AssertionType.FRESHNESS;

          // We only need to fetch the latest anomaly event timestamp if we're actually going to
          // create new anomaly events. Avoid calling out to GMS when there is no work to do (e.g.
          // no run events / no operations in the time range).
          final long currentTimeMillis = _timeProvider.getAsLong();
          long latestAnomalyEventTimestampMillis = currentTimeMillis;

          // 3. For freshness assertions, get operations instead of run events
          final List<MonitorAnomalyEvent> anomalyEvents = new ArrayList<>();
          final List<MetadataChangeProposal> proposals = new ArrayList<>();

          if (isFreshnessAssertion) {
            // For freshness assertions, we mark Operations as anomalies
            if (assertionInfo == null || !assertionInfo.hasFreshnessAssertion()) {
              log.warn(
                  "Freshness assertion {} does not have freshness assertion info", assertionUrn);
              return new ArrayList<>();
            }

            // Get entity URN from freshness assertion info
            final Urn entityUrn = assertionInfo.getFreshnessAssertion().getEntity();
            log.info(
                "Processing freshness assertion {} for entity {} in time range {} to {}",
                assertionUrn,
                entityUrn,
                startTimeMillis,
                endTimeMillis);

            try {
              final List<EnvelopedAspect> operationAspects =
                  fetchOperationAspects(queryContext, entityUrn, startTimeMillis, endTimeMillis);

              if (operationAspects.isEmpty()) {
                log.info(
                    "No operations found for entity {} in time range {} to {}",
                    entityUrn,
                    startTimeMillis,
                    endTimeMillis);
                return new ArrayList<>();
              }

              // We have operations to process; now fetch the latest anomaly event timestamp so we
              // can generate monotonically increasing timestamps for new feedback events.
              latestAnomalyEventTimestampMillis =
                  getLatestAnomalyTimestampWithFallback(
                      queryContext, monitorUrn, currentTimeMillis);

              // Fetch existing anomaly events to avoid duplicates
              final List<MonitorAnomalyEvent> existingAnomalyEvents =
                  MonitorAnomalyEventUtils.fetchAnomalyEvents(
                      queryContext.getOperationContext(),
                      _entityClient,
                      monitorUrn.toString(),
                      startTimeMillis,
                      endTimeMillis);

              // Build a map of existing anomalies by sourceEventTimestampMillis
              final Map<Long, MonitorAnomalyEvent> existingAnomalyMap =
                  MonitorAnomalyEventUtils.buildAnomalyEventMapBySourceEvent(existingAnomalyEvents);

              // For each operation, construct the anomaly event (only if it doesn't already exist
              // or state differs)
              for (EnvelopedAspect aspect : operationAspects) {
                final Operation operation =
                    GenericRecordUtils.deserializeAspect(
                        aspect.getAspect().getValue(),
                        aspect.getAspect().getContentType(),
                        Operation.class);

                final long operationTimestamp = operation.getLastUpdatedTimestamp();

                // Check if an anomaly event already exists for this operation timestamp
                final MonitorAnomalyEvent existingAnomalyEvent =
                    existingAnomalyMap.get(operationTimestamp);
                if (existingAnomalyEvent != null) {
                  // If the state is the same, skip to avoid creating duplicate entries
                  if (existingAnomalyEvent.hasState() && existingAnomalyEvent.getState() == state) {
                    log.info(
                        "Anomaly event already exists for operation {} (timestamp {}) with same state {}, skipping",
                        operationTimestamp,
                        operationTimestamp,
                        state);
                    continue;
                  }
                  // If the state is different, create a new entry to update the state
                  // (this allows "unmarking" anomalies by changing from CONFIRMED to REJECTED)
                  log.info(
                      "Anomaly event exists for operation {} (timestamp {}) with state {}, updating to state {}",
                      operationTimestamp,
                      operationTimestamp,
                      existingAnomalyEvent.hasState() ? existingAnomalyEvent.getState() : "null",
                      state);
                }

                latestAnomalyEventTimestampMillis++;
                final MonitorAnomalyEvent anomalyEvent =
                    MonitorAnomalyEventUtils.buildFreshnessAnomalyEvent(
                        assertionUrn, operationTimestamp, state, latestAnomalyEventTimestampMillis);
                log.info(
                    "Creating freshness anomaly event for operation {} with timestamp {} and state {}",
                    operationTimestamp,
                    latestAnomalyEventTimestampMillis,
                    state);

                anomalyEvents.add(anomalyEvent);

                // Build MCP for each anomaly event
                proposals.add(
                    AspectUtils.buildSynchronousMetadataChangeProposal(
                        monitorUrn, Constants.MONITOR_ANOMALY_EVENT_ASPECT_NAME, anomalyEvent));
              }
            } catch (RemoteInvocationException exception) {
              throw new DataHubGraphQLException(
                  String.format(
                      "Failed to get operations for entity %s in time range %s to %s. Error: %s",
                      entityUrn, startTimeMillis, endTimeMillis, exception.getMessage()),
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            }
          } else {
            // For non-freshness assertions, use run events (existing logic)
            final List<AssertionRunEvent> runEvents =
                _assertionService.getAssertionRunEvents(
                    queryContext.getOperationContext(),
                    assertionUrn,
                    startTimeMillis,
                    endTimeMillis);

            if (runEvents.isEmpty()) {
              log.info(
                  "No run events found for assertion {} in time range {} to {}",
                  assertionUrn,
                  startTimeMillis,
                  endTimeMillis);
              return new ArrayList<>();
            }

            // We have run events to process; now fetch the latest anomaly event timestamp so we
            // can generate monotonically increasing timestamps for new feedback events.
            latestAnomalyEventTimestampMillis =
                getLatestAnomalyTimestampWithFallback(queryContext, monitorUrn, currentTimeMillis);

            // Fetch existing anomaly events to avoid duplicates
            Map<Long, MonitorAnomalyEvent> existingAnomalyMap = new HashMap<>();
            try {
              final List<MonitorAnomalyEvent> existingAnomalyEvents =
                  MonitorAnomalyEventUtils.fetchAnomalyEvents(
                      queryContext.getOperationContext(),
                      _entityClient,
                      monitorUrn.toString(),
                      startTimeMillis,
                      endTimeMillis);

              // Build a map of existing anomalies by sourceEventTimestampMillis
              existingAnomalyMap =
                  MonitorAnomalyEventUtils.buildAnomalyEventMapBySourceEvent(existingAnomalyEvents);
            } catch (RemoteInvocationException exception) {
              log.warn(
                  "Failed to fetch existing anomaly events for monitor {}. Proceeding without deduplication. Error: {}",
                  monitorUrn,
                  exception.getMessage());
              // Continue without deduplication if fetch fails
            }

            // For each run event, construct the anomaly event (only if it doesn't already exist)
            for (AssertionRunEvent runEvent : runEvents) {
              final long runEventTimestamp = runEvent.getTimestampMillis();

              // Check if an anomaly event already exists for this run event timestamp
              if (existingAnomalyMap.containsKey(runEventTimestamp)) {
                log.info(
                    "Anomaly event already exists for run event {} (timestamp {}), skipping",
                    runEventTimestamp,
                    runEventTimestamp);
                continue;
              }
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
          }

          // If we ended up with nothing to ingest (e.g. all events already had identical feedback),
          // avoid calling batch ingest / retraining.
          if (proposals.isEmpty()) {
            log.info(
                "No anomaly feedback updates generated for monitor {} in time range {} to {}",
                monitorUrn,
                startTimeMillis,
                endTimeMillis);
            return new ArrayList<>();
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

  private long getLatestAnomalyTimestampWithFallback(
      @Nonnull final QueryContext queryContext,
      @Nonnull final Urn monitorUrn,
      final long currentTimeMillis) {
    try {
      final long fetchedLatestTimestamp =
          MonitorAnomalyEventUtils.getLatestAnomalyEventTimestamp(
              queryContext.getOperationContext(), this._entityClient, monitorUrn);
      final long latestAnomalyEventTimestampMillis =
          Math.max(currentTimeMillis, fetchedLatestTimestamp);
      log.info(
          "Using max timestamp for monitor {}: current time = {}, fetched latest = {}, using = {}",
          monitorUrn,
          currentTimeMillis,
          fetchedLatestTimestamp,
          latestAnomalyEventTimestampMillis);
      return latestAnomalyEventTimestampMillis;
    } catch (RemoteInvocationException exception) {
      log.warn(
          "Failed to get latest anomaly event timestamp for monitor {}. Using current time as fallback. Error: {}",
          monitorUrn,
          exception.getMessage());
      return currentTimeMillis;
    }
  }

  /**
   * Fetches {@link Operation} time series aspects for a target entity within a time range.
   *
   * <p>Note: We filter by {@link Constants#OPERATION_EVENT_TIME_FIELD_NAME} (backed by {@code
   * lastUpdatedTimestamp}) rather than {@code timestampMillis}, so we pass {@code null} for
   * timestampMillis filtering and apply an explicit {@link Filter} instead.
   */
  private List<EnvelopedAspect> fetchOperationAspects(
      @Nonnull final QueryContext queryContext,
      @Nonnull final Urn entityUrn,
      final long startTimeMillis,
      final long endTimeMillis)
      throws RemoteInvocationException {
    final Filter operationFilter = buildOperationFilter(startTimeMillis, endTimeMillis);
    return _entityClient.getTimeseriesAspectValues(
        queryContext.getOperationContext(),
        entityUrn.toString(),
        entityUrn.getEntityType(),
        Constants.OPERATION_ASPECT_NAME,
        null, // Don't use timestampMillis filtering
        null, // Don't use timestampMillis filtering
        null, // no limit
        operationFilter); // Filter by lastUpdatedTimestamp
  }

  /**
   * Builds a filter for the {@link Operation} time series aspect that constrains:
   *
   * <ul>
   *   <li>Operation time to within [{@code startTimeMillis}, {@code endTimeMillis}] (inclusive)
   *   <li>Operation type to INSERT / CREATE / UPDATE
   * </ul>
   */
  private static Filter buildOperationFilter(final long startTimeMillis, final long endTimeMillis) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            ImmutableList.of(
                                new Criterion()
                                    .setField(Constants.OPERATION_EVENT_TIME_FIELD_NAME)
                                    .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
                                    .setValues(
                                        new StringArray(
                                            ImmutableList.of(String.valueOf(startTimeMillis)))),
                                new Criterion()
                                    .setField(Constants.OPERATION_EVENT_TIME_FIELD_NAME)
                                    .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
                                    .setValues(
                                        new StringArray(
                                            ImmutableList.of(String.valueOf(endTimeMillis)))),
                                new Criterion()
                                    .setField("operationType")
                                    .setCondition(Condition.IN)
                                    .setValues(
                                        new StringArray(
                                            ImmutableList.of(
                                                OperationType.INSERT.name(),
                                                OperationType.CREATE.name(),
                                                OperationType.UPDATE.name()))))))));
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
