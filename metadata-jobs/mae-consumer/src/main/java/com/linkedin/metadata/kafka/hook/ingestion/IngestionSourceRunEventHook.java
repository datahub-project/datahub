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
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.ingestion.IngestionSourceRunEvents;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
@Import({EntityRegistryFactory.class, RestliEntityClientFactory.class})
public class IngestionSourceRunEventHook implements MetadataChangeLogHook {

  // RESTATE is excluded so restore-indices replays of old runs do not rewrite the latest run.
  private static final Set<ChangeType> SUPPORTED_CHANGE_TYPES =
      Set.of(ChangeType.UPSERT, ChangeType.CREATE, ChangeType.CREATE_ENTITY);
  private static final String INGESTION_SOURCE_FIELD = "ingestionSource";
  private static final String REQUEST_TIME_FIELD = "requestTimeMs";

  private final SystemEntityClient systemEntityClient;
  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public IngestionSourceRunEventHook(
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull @Value("${ingestionSourceRunEvents.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${ingestionSourceRunEvents.hook.consumerGroupSuffix}")
          String consumerGroupSuffix) {
    this.systemEntityClient = systemEntityClient;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public IngestionSourceRunEventHook(
      @Nonnull final SystemEntityClient systemEntityClient, @Nonnull Boolean isEnabled) {
    this(systemEntityClient, isEnabled, "");
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
    if (!isEnabled
        || !EXECUTION_REQUEST_ENTITY_NAME.equals(event.getEntityType())
        || !SUPPORTED_CHANGE_TYPES.contains(event.getChangeType())
        || event.getAspect() == null) {
      return;
    }
    Urn executionRequestUrn = event.getEntityUrn();
    try {
      if (EXECUTION_REQUEST_INPUT_ASPECT_NAME.equals(event.getAspectName())) {
        handleRequested(operationContext, executionRequestUrn, event);
      } else if (EXECUTION_REQUEST_RESULT_ASPECT_NAME.equals(event.getAspectName())) {
        handleResult(operationContext, executionRequestUrn, event);
      }
    } catch (Exception e) {
      log.error(
          "Failed to record an ingestion source run event for execution request {} ({})",
          executionRequestUrn,
          event.getAspectName(),
          e);
    }
  }

  private void handleRequested(
      @Nonnull OperationContext operationContext,
      @Nonnull Urn executionRequestUrn,
      @Nonnull MetadataChangeLog event)
      throws Exception {
    if (event.getPreviousAspectValue() != null) {
      return;
    }
    ExecutionRequestInput input = aspect(event, false, ExecutionRequestInput.class);
    record(operationContext, executionRequestUrn, input, null, eventTime(event));
  }

  private void handleResult(
      @Nonnull OperationContext operationContext,
      @Nonnull Urn executionRequestUrn,
      @Nonnull MetadataChangeLog event)
      throws Exception {
    ExecutionRequestResult result = aspect(event, false, ExecutionRequestResult.class);
    ExecutionRequestResult previous = aspect(event, true, ExecutionRequestResult.class);
    // The executor rewrites a running result every few seconds; only a status change is an event.
    if (previous != null && Objects.equals(previous.getStatus(), result.getStatus())) {
      return;
    }
    record(
        operationContext,
        executionRequestUrn,
        executionRequestInput(operationContext, executionRequestUrn),
        result,
        eventTime(event));
  }

  /**
   * MCLs for different runs of a source arrive on different partitions, so an older run's event can
   * be processed after a newer run's. The source's search document keeps whichever event was
   * written last, so a run is only recorded while it is the source's newest: no event for a later
   * request has been recorded and no later request is indexed. Both checks read search, so two runs
   * of one source changing status within the index refresh interval can still race.
   */
  private void record(
      @Nonnull OperationContext operationContext,
      @Nonnull Urn executionRequestUrn,
      @Nullable ExecutionRequestInput input,
      @Nullable ExecutionRequestResult result,
      long timestampMillis)
      throws Exception {
    Urn sourceUrn = IngestionSourceRunEvents.ingestionSourceUrn(input);
    if (sourceUrn == null || !sourceExists(operationContext, sourceUrn)) {
      return;
    }
    DataHubIngestionSourceRunEvent latest = latestEvent(operationContext, sourceUrn);
    if (IngestionSourceRunEvents.isSupersededBy(input, latest)
        || hasNewerRequest(operationContext, sourceUrn, executionRequestUrn, input)) {
      return;
    }
    systemEntityClient.ingestProposal(
        operationContext,
        AspectUtils.buildMetadataChangeProposal(
            sourceUrn,
            INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME,
            IngestionSourceRunEvents.buildEvent(
                executionRequestUrn, input, result, timestampMillis, latest)),
        false);
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

  private boolean hasNewerRequest(
      @Nonnull OperationContext operationContext,
      @Nonnull Urn sourceUrn,
      @Nonnull Urn executionRequestUrn,
      @Nonnull ExecutionRequestInput input)
      throws Exception {
    SearchResult newest =
        systemEntityClient.filter(
            operationContext,
            EXECUTION_REQUEST_ENTITY_NAME,
            QueryUtils.newFilter(INGESTION_SOURCE_FIELD, sourceUrn.toString()),
            List.of(
                new SortCriterion().setField(REQUEST_TIME_FIELD).setOrder(SortOrder.DESCENDING)),
            0,
            1);
    // A request that was just created may not be indexed yet, so only a different, later
    // request counts as newer.
    if (newest.getEntities().isEmpty()
        || newest.getEntities().get(0).getEntity().equals(executionRequestUrn)) {
      return false;
    }
    ExecutionRequestInput newestInput =
        executionRequestInput(operationContext, newest.getEntities().get(0).getEntity());
    return newestInput != null && newestInput.getRequestedAt() > input.getRequestedAt();
  }

  private boolean sourceExists(@Nonnull OperationContext operationContext, @Nonnull Urn sourceUrn)
      throws Exception {
    EntityResponse source =
        systemEntityClient.getV2(
            operationContext,
            INGESTION_SOURCE_ENTITY_NAME,
            sourceUrn,
            Set.of(INGESTION_INFO_ASPECT_NAME),
            false);
    return source != null && source.getAspects().containsKey(INGESTION_INFO_ASPECT_NAME);
  }

  @Nullable
  private ExecutionRequestInput executionRequestInput(
      @Nonnull OperationContext operationContext, @Nonnull Urn executionRequestUrn)
      throws Exception {
    EntityResponse response =
        systemEntityClient.getV2(
            operationContext,
            EXECUTION_REQUEST_ENTITY_NAME,
            executionRequestUrn,
            Set.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME),
            false);
    EnvelopedAspect input =
        response != null ? response.getAspects().get(EXECUTION_REQUEST_INPUT_ASPECT_NAME) : null;
    return input != null ? new ExecutionRequestInput(input.getValue().data()) : null;
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
}
