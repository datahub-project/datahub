package com.linkedin.metadata.kafka.hook.ingestion;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;

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
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.ingestion.IngestionSourceRunEvents;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LatestIngestionRuns;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.HashMap;
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
 * Records a {@code dataHubIngestionSourceRunEvent} on an ingestion source when one of its execution
 * requests is created or changes status, so the source can be sorted and filtered by its latest
 * run.
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

  // RESTATE is excluded so restore-indices replays of old runs do not rewrite the latest run.
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
    if (changes.isEmpty()) {
      return;
    }
    try {
      record(operationContext, changes);
    } catch (Exception e) {
      failed(operationContext, changes.size());
      log.error(
          "Failed to record ingestion source run events for execution requests {}",
          changes.stream().map(RunChange::executionRequest).collect(Collectors.toList()),
          e);
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
              event.getEntityUrn(),
              aspect(event, false, ExecutionRequestInput.class),
              null,
              eventTime(event))
          : null;
    }
    if (EXECUTION_REQUEST_RESULT_ASPECT_NAME.equals(event.getAspectName())) {
      ExecutionRequestResult result = aspect(event, false, ExecutionRequestResult.class);
      ExecutionRequestResult previous = aspect(event, true, ExecutionRequestResult.class);
      // The executor rewrites a running result every few seconds; only a status change is an
      // event.
      return previous == null || !Objects.equals(previous.getStatus(), result.getStatus())
          ? new RunChange(event.getEntityUrn(), null, result, eventTime(event))
          : null;
    }
    return null;
  }

  /**
   * MCLs for different runs of a source arrive on different partitions, so an older run's event can
   * be processed after a newer run's. A run is only recorded while it is the source's newest: no
   * event for a later request has been recorded and no later request is indexed. Both checks read
   * search, so two runs of one source changing status within the index refresh interval can still
   * race.
   */
  private void record(@Nonnull OperationContext operationContext, @Nonnull List<RunChange> changes)
      throws Exception {
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

    Set<Urn> sourceUrns = existingSources(operationContext, sourceUrns(changes, inputs));
    if (sourceUrns.isEmpty()) {
      return;
    }
    Map<Urn, LatestIngestionRuns> latestRuns =
        entitySearchService.getLatestIngestionRuns(operationContext, sourceUrns);
    inputs.putAll(
        executionRequestInputs(
            operationContext,
            latestRuns.values().stream()
                .map(LatestIngestionRuns::getLatestRun)
                .filter(urn -> !inputs.containsKey(urn))
                .collect(Collectors.toSet())));
    Map<Urn, DataHubIngestionSourceRunEvent> latestEvents = new HashMap<>();
    for (Urn sourceUrn : sourceUrns) {
      latestEvents.put(sourceUrn, latestEvent(operationContext, sourceUrn));
    }

    for (RunChange change : changes) {
      ExecutionRequestInput input = inputs.get(change.executionRequest());
      Urn sourceUrn = IngestionSourceRunEvents.ingestionSourceUrn(input);
      if (sourceUrn == null || !sourceUrns.contains(sourceUrn)) {
        continue;
      }
      DataHubIngestionSourceRunEvent latest = latestEvents.get(sourceUrn);
      if (IngestionSourceRunEvents.isSupersededBy(input, latest)
          || hasNewerRequest(change, input, latestRuns.get(sourceUrn), inputs)) {
        continue;
      }
      DataHubIngestionSourceRunEvent event =
          IngestionSourceRunEvents.buildEvent(
              change.executionRequest(), input, change.result(), change.timestampMillis(), latest);
      try {
        systemEntityClient.ingestProposal(
            operationContext,
            AspectUtils.buildMetadataChangeProposal(
                sourceUrn, INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME, event),
            false);
        // Later changes in this batch compare with this event; search does not have it yet.
        latestEvents.put(sourceUrn, event);
      } catch (Exception e) {
        failed(operationContext, 1);
        log.error(
            "Failed to record a {} run event for execution request {} on ingestion source {}",
            event.getStatus(),
            change.executionRequest(),
            sourceUrn,
            e);
      }
    }
  }

  @Nonnull
  private static Set<Urn> sourceUrns(
      @Nonnull List<RunChange> changes, @Nonnull Map<Urn, ExecutionRequestInput> inputs) {
    return changes.stream()
        .map(
            change ->
                IngestionSourceRunEvents.ingestionSourceUrn(inputs.get(change.executionRequest())))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  /**
   * A request that was just created may not be indexed yet, so only a different, later request
   * counts as newer.
   */
  private static boolean hasNewerRequest(
      @Nonnull RunChange change,
      @Nonnull ExecutionRequestInput input,
      @Nullable LatestIngestionRuns latestRuns,
      @Nonnull Map<Urn, ExecutionRequestInput> inputs) {
    if (latestRuns == null || latestRuns.getLatestRun().equals(change.executionRequest())) {
      return false;
    }
    ExecutionRequestInput newestInput = inputs.get(latestRuns.getLatestRun());
    return newestInput != null && newestInput.getRequestedAt() > input.getRequestedAt();
  }

  @Nullable
  private DataHubIngestionSourceRunEvent latestEvent(
      @Nonnull OperationContext operationContext, @Nonnull Urn sourceUrn) throws Exception {
    List<com.linkedin.metadata.aspect.EnvelopedAspect> latest =
        systemEntityClient.getTimeseriesAspectValues(
            operationContext,
            sourceUrn.toString(),
            INGESTION_SOURCE_ENTITY_NAME,
            INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME,
            null,
            null,
            1,
            null);
    if (latest.isEmpty()) {
      return null;
    }
    GenericAspect value = latest.get(0).getAspect();
    return GenericRecordUtils.deserializeAspect(
        value.getValue(), value.getContentType(), DataHubIngestionSourceRunEvent.class);
  }

  /** Sources that still exist; an event for a deleted source would recreate it. */
  @Nonnull
  private Set<Urn> existingSources(
      @Nonnull OperationContext operationContext, @Nonnull Set<Urn> sourceUrns) throws Exception {
    if (sourceUrns.isEmpty()) {
      return Set.of();
    }
    return systemEntityClient
        .batchGetV2(operationContext, sourceUrns, Set.of(INGESTION_INFO_ASPECT_NAME))
        .entrySet()
        .stream()
        .filter(source -> source.getValue().getAspects().containsKey(INGESTION_INFO_ASPECT_NAME))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  @Nonnull
  private Map<Urn, ExecutionRequestInput> executionRequestInputs(
      @Nonnull OperationContext operationContext, @Nonnull Set<Urn> executionRequestUrns)
      throws Exception {
    Map<Urn, ExecutionRequestInput> inputs = new HashMap<>();
    if (executionRequestUrns.isEmpty()) {
      return inputs;
    }
    systemEntityClient
        .batchGetV2(
            operationContext, executionRequestUrns, Set.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
        .forEach(
            (urn, response) -> {
              EnvelopedAspect input =
                  response.getAspects().get(EXECUTION_REQUEST_INPUT_ASPECT_NAME);
              if (input != null) {
                inputs.put(urn, new ExecutionRequestInput(input.getValue().data()));
              }
            });
    return inputs;
  }

  private static void failed(@Nonnull OperationContext operationContext, int count) {
    operationContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                metricUtils.increment(IngestionSourceRunEventHook.class, FAILURE_METRIC, count));
  }

  private static long eventTime(@Nonnull MetadataChangeLog event) {
    return event.hasCreated() ? event.getCreated().getTime() : System.currentTimeMillis();
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
      @Nullable ExecutionRequestResult result,
      long timestampMillis) {}
}
