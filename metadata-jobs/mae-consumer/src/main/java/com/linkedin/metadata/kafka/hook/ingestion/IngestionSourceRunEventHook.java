package com.linkedin.metadata.kafka.hook.ingestion;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.ingestion.IngestionSourceRunEvents;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * When one of an ingestion source's execution requests is created or changes status, records a
 * {@code dataHubIngestionSourceRunEvent} for the run and updates the source's {@code
 * dataHubIngestionSourceRunSummary}, so the source can be sorted and filtered by its latest run.
 *
 * <p>This is a hook rather than a post-commit MCP side effect because side-effect writes are
 * ingested through the async MCP path, which drops timeseries aspects.
 */
@Slf4j
@Component
@Import({
  EntityRegistryFactory.class,
  RestliEntityClientFactory.class,
  EntitySearchServiceFactory.class
})
public class IngestionSourceRunEventHook implements MetadataChangeLogHook {

  // RESTATE is excluded so restore-indices replays of old runs do not add history events.
  private static final Set<ChangeType> SUPPORTED_CHANGE_TYPES =
      Set.of(ChangeType.UPSERT, ChangeType.CREATE, ChangeType.CREATE_ENTITY);
  private static final String FAILURE_METRIC = "ingestionSourceRunEvent_failure";

  private final SystemEntityClient systemEntityClient;
  private final EntitySearchService entitySearchService;
  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public IngestionSourceRunEventHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final EntitySearchService entitySearchService,
      @Nonnull @Value("${ingestionSourceRunEvents.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${ingestionSourceRunEvents.hook.consumerGroupSuffix}")
          String consumerGroupSuffix) {
    this.systemEntityClient = systemEntityClient;
    this.entitySearchService = entitySearchService;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public IngestionSourceRunEventHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull final EntitySearchService entitySearchService,
      @Nonnull Boolean isEnabled) {
    this(systemEntityClient, entitySearchService, isEnabled, "");
  }

  @Override
  public IngestionSourceRunEventHook init(@Nonnull OperationContext systemOperationContext) {
    log.info("Initialized the ingestion source run event hook");
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(
      @Nonnull final OperationContext operationContext, @Nonnull final MetadataChangeLog event) {
    invokeBatch(operationContext, List.of(event));
  }

  /**
   * A failed read propagates, so the batch is reported as failed; a failed write costs only its own
   * run and is counted in {@code ingestionSourceRunEvent_failure}.
   */
  @Override
  public void invokeBatch(
      @Nonnull final OperationContext operationContext,
      @Nonnull final Collection<MetadataChangeLog> events) {
    if (!isEnabled) {
      return;
    }
    List<RunChange> changes =
        events.stream()
            .map(IngestionSourceRunEventHook::runChange)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    if (!changes.isEmpty()) {
      record(operationContext, changes);
    }
  }

  /**
   * The run change an execution-request MCL reports, or null when it reports none: a new request,
   * or a result whose status changed. Decided from the MCL alone, so most MCLs need no reads.
   */
  @Nullable
  private static RunChange runChange(@Nonnull MetadataChangeLog event) {
    if (!EXECUTION_REQUEST_ENTITY_NAME.equals(event.getEntityType())
        || !SUPPORTED_CHANGE_TYPES.contains(event.getChangeType())
        || event.getAspect() == null) {
      return null;
    }
    if (EXECUTION_REQUEST_INPUT_ASPECT_NAME.equals(event.getAspectName())) {
      return event.getPreviousAspectValue() == null
          ? new RunChange(
              event.getEntityUrn(), aspect(event, false, ExecutionRequestInput.class), null)
          : null;
    }
    if (EXECUTION_REQUEST_RESULT_ASPECT_NAME.equals(event.getAspectName())) {
      ExecutionRequestResult result = aspect(event, false, ExecutionRequestResult.class);
      ExecutionRequestResult previous = aspect(event, true, ExecutionRequestResult.class);
      // The executor rewrites a running result every few seconds; only a status change is an
      // event.
      return previous == null || !Objects.equals(previous.getStatus(), result.getStatus())
          ? new RunChange(event.getEntityUrn(), null, result)
          : null;
    }
    return null;
  }

  private void record(
      @Nonnull OperationContext operationContext, @Nonnull List<RunChange> changes) {
    Map<Urn, ExecutionRequestInput> inputs =
        executionRequestInputs(
            operationContext,
            changes.stream()
                .filter(change -> change.input() == null)
                .map(RunChange::executionRequest)
                .collect(Collectors.toSet()));
    changes.forEach(
        change -> {
          if (change.input() != null) {
            inputs.put(change.executionRequest(), change.input());
          }
        });

    Map<Urn, DataHubIngestionSourceInfo> sources =
        existingSources(
            operationContext,
            changes.stream()
                .map(change -> sourceUrn(change, inputs))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    Map<Urn, Pair<Urn, Long>> successesAfterRollback =
        successesAfterRollback(operationContext, changes, inputs);

    for (RunChange change : changes) {
      Urn sourceUrn = sourceUrn(change, inputs);
      DataHubIngestionSourceInfo source = sourceUrn != null ? sources.get(sourceUrn) : null;
      if (source == null) {
        continue;
      }
      ExecutionRequestInput input = inputs.get(change.executionRequest());
      DataHubIngestionSourceRunSummary summary =
          IngestionSourceRunEvents.buildSummary(change.executionRequest(), input, change.result());
      Pair<Urn, Long> successAfterRollback = successesAfterRollback.get(change.executionRequest());
      if (successAfterRollback != null) {
        IngestionSourceRunEvents.setLastSuccess(
            summary, successAfterRollback.getFirst(), successAfterRollback.getSecond());
      }
      // The summary and the event are separate writes (a batch mixing a versioned and a timeseries
      // aspect is not one transaction). The summary goes first: it is what the source list reads,
      // and if the event write then fails only one history entry is missing.
      try {
        ingest(operationContext, sourceUrn, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME, summary);
        ingest(
            operationContext,
            sourceUrn,
            INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME,
            IngestionSourceRunEvents.buildEvent(
                change.executionRequest(), input, change.result(), source.getType()));
      } catch (Exception e) {
        failed(operationContext);
        log.error(
            "Failed to record a {} run for execution request {} on ingestion source {}",
            summary.getLastRunStatus(),
            change.executionRequest(),
            sourceUrn,
            e);
      }
    }
  }

  private void ingest(
      @Nonnull OperationContext operationContext,
      @Nonnull Urn sourceUrn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect)
      throws Exception {
    systemEntityClient.batchIngestProposals(
        operationContext,
        List.of(AspectUtils.buildMetadataChangeProposal(sourceUrn, aspectName, aspect)),
        false);
  }

  /**
   * For each run being rolled back, keyed by its execution request, its source's latest other
   * successful run and that run's request time, read from the execution request index with the
   * rolled-back runs left out. That replaces the rolled-back run if it was the last success (see
   * {@link IngestionSourceRunEvents#merge}); a run with no entry leaves the source with no last
   * success.
   */
  @Nonnull
  private Map<Urn, Pair<Urn, Long>> successesAfterRollback(
      @Nonnull OperationContext operationContext,
      @Nonnull List<RunChange> changes,
      @Nonnull Map<Urn, ExecutionRequestInput> inputs) {
    List<RunChange> rollbacks =
        changes.stream()
            .filter(
                change ->
                    change.result() != null
                        && IngestionSourceRunEvents.ROLLBACK_STATUSES.contains(
                            change.result().getStatus())
                        && sourceUrn(change, inputs) != null)
            .collect(Collectors.toList());
    Map<Urn, Pair<Urn, Long>> successes = new HashMap<>();
    if (rollbacks.isEmpty()) {
      return successes;
    }
    Map<Urn, Urn> latestSuccesses = new HashMap<>();
    entitySearchService
        .getLatestIngestionRuns(
            operationContext,
            rollbacks.stream().map(change -> sourceUrn(change, inputs)).collect(Collectors.toSet()),
            rollbacks.stream().map(RunChange::executionRequest).collect(Collectors.toSet()))
        .forEach(
            (sourceUrn, runs) -> {
              if (runs.getLatestSuccessfulRun() != null) {
                latestSuccesses.put(sourceUrn, runs.getLatestSuccessfulRun());
              }
            });
    Map<Urn, ExecutionRequestInput> successInputs =
        executionRequestInputs(operationContext, new HashSet<>(latestSuccesses.values()));
    for (RunChange rollback : rollbacks) {
      Urn latestSuccess = latestSuccesses.get(sourceUrn(rollback, inputs));
      ExecutionRequestInput replacement =
          latestSuccess != null ? successInputs.get(latestSuccess) : null;
      if (replacement != null) {
        successes.put(
            rollback.executionRequest(), Pair.of(latestSuccess, replacement.getRequestedAt()));
      }
    }
    return successes;
  }

  @Nullable
  private static Urn sourceUrn(
      @Nonnull RunChange change, @Nonnull Map<Urn, ExecutionRequestInput> inputs) {
    return IngestionSourceRunEvents.ingestionSourceUrn(inputs.get(change.executionRequest()));
  }

  /** Sources that still exist; a write for a deleted source would recreate it. */
  @Nonnull
  private Map<Urn, DataHubIngestionSourceInfo> existingSources(
      @Nonnull OperationContext operationContext, @Nonnull Set<Urn> sourceUrns) {
    Map<Urn, DataHubIngestionSourceInfo> sources = new HashMap<>();
    batchGet(operationContext, sourceUrns, INGESTION_INFO_ASPECT_NAME)
        .forEach(
            (urn, info) ->
                sources.put(urn, new DataHubIngestionSourceInfo(info.getValue().data())));
    return sources;
  }

  @Nonnull
  private Map<Urn, ExecutionRequestInput> executionRequestInputs(
      @Nonnull OperationContext operationContext, @Nonnull Set<Urn> executionRequestUrns) {
    Map<Urn, ExecutionRequestInput> inputs = new HashMap<>();
    batchGet(operationContext, executionRequestUrns, EXECUTION_REQUEST_INPUT_ASPECT_NAME)
        .forEach(
            (urn, input) -> inputs.put(urn, new ExecutionRequestInput(input.getValue().data())));
    return inputs;
  }

  @Nonnull
  private Map<Urn, EnvelopedAspect> batchGet(
      @Nonnull OperationContext operationContext,
      @Nonnull Set<Urn> urns,
      @Nonnull String aspectName) {
    Map<Urn, EnvelopedAspect> aspects = new HashMap<>();
    if (urns.isEmpty()) {
      return aspects;
    }
    try {
      systemEntityClient
          .batchGetV2(operationContext, urns, Set.of(aspectName))
          .forEach(
              (urn, response) -> {
                EnvelopedAspect aspect = response.getAspects().get(aspectName);
                if (aspect != null) {
                  aspects.put(urn, aspect);
                }
              });
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read " + aspectName + " for " + urns, e);
    }
    return aspects;
  }

  private static void failed(@Nonnull OperationContext operationContext) {
    operationContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                metricUtils.increment(IngestionSourceRunEventHook.class, FAILURE_METRIC, 1));
  }

  @Nullable
  private static <T extends RecordTemplate> T aspect(
      @Nonnull MetadataChangeLog event, boolean previous, @Nonnull Class<T> clazz) {
    GenericAspect value = previous ? event.getPreviousAspectValue() : event.getAspect();
    return value != null
        ? GenericRecordUtils.deserializeAspect(value.getValue(), value.getContentType(), clazz)
        : null;
  }

  /**
   * @param input null for a result change; read from the execution request
   * @param result null for a new request, which has no result yet
   */
  private record RunChange(
      @Nonnull Urn executionRequest,
      @Nullable ExecutionRequestInput input,
      @Nullable ExecutionRequestResult result) {}
}
