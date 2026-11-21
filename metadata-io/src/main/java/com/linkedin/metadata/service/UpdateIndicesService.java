package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.entity.SearchIndicesService;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.UpdateIndicesV3Strategy;
import com.linkedin.metadata.search.elasticsearch.update.BulkTransferException;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.UnknownDataGuard;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateIndicesService implements SearchIndicesService {
  private static final UnknownDataGuard UNKNOWN_DATA =
      UnknownDataGuard.forSite(UpdateIndicesService.class, "MCL");

  @VisibleForTesting @Getter private final UpdateGraphIndicesService updateGraphIndicesService;
  private final ElasticSearchService elasticSearchService;
  private final SystemMetadataService systemMetadataService;
  @Nullable private final TimeseriesWriteThrottleCache timeseriesThrottleCache;

  @Getter private final boolean searchDiffMode;

  @Getter private final boolean structuredPropertiesHookEnabled;

  @Getter private final boolean structuredPropertiesWriteEnabled;

  // Update indices strategies
  private final Collection<UpdateIndicesStrategy> updateStrategies;

  private static final String DOCUMENT_TRANSFORM_FAILED_METRIC = "document_transform_failed";
  private static final String SEARCH_DIFF_MODE_SKIPPED_METRIC = "search_diff_no_changes_detected";
  private static final String INVALID_MCL_SKIPPED_METRIC = "invalid_mcl_skipped";
  private static final String PREVIOUS_FROM_NEWER_VERSION_OMITTED_METRIC =
      "mcl_previous_from_newer_version_omitted";

  public static final Set<ChangeType> UPDATE_CHANGE_TYPES =
      ImmutableSet.of(
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.UPSERT,
          ChangeType.RESTATE,
          ChangeType.PATCH);

  public UpdateIndicesService(
      UpdateGraphIndicesService updateGraphIndicesService,
      ElasticSearchService elasticSearchService,
      SystemMetadataService systemMetadataService,
      @Nonnull Collection<UpdateIndicesStrategy> updateStrategies,
      @Nullable TimeseriesWriteThrottleCache timeseriesThrottleCache,
      boolean searchDiffMode,
      boolean structuredPropertiesHookEnabled,
      boolean structuredPropertiesWriteEnabled) {
    this.updateGraphIndicesService = updateGraphIndicesService;
    this.elasticSearchService = elasticSearchService;
    this.systemMetadataService = systemMetadataService;
    this.updateStrategies = updateStrategies;
    this.timeseriesThrottleCache = timeseriesThrottleCache;
    this.searchDiffMode = searchDiffMode;
    this.structuredPropertiesHookEnabled = structuredPropertiesHookEnabled;
    this.structuredPropertiesWriteEnabled = structuredPropertiesWriteEnabled;
  }

  @Override
  public void handleChangeEvent(
      @Nonnull OperationContext opContext, @Nonnull MetadataChangeLog metadataChangeLog) {
    handleChangeEvents(opContext, Collections.singletonList(metadataChangeLog));
  }

  @Override
  public void handleChangeEvents(
      @Nonnull OperationContext opContext, @Nonnull final Collection<MetadataChangeLog> events) {
    final GroupedEvents grouped = groupForStrategies(opContext, events);
    LinkedHashMap<Urn, List<MCLItem>> groupedEvents = grouped.byUrn();

    RuntimeException strategyFailure = null;
    for (UpdateIndicesStrategy strategy : updateStrategies) {
      if (strategy.isEnabled()) {
        try {
          strategy.processBatch(opContext, groupedEvents, structuredPropertiesHookEnabled);
        } catch (RuntimeException e) {
          log.error("Update indices strategy failed; continuing required side effects", e);
          strategyFailure = accumulateStrategyFailure(strategyFailure, e);
        }
      }
    }

    // A failed strategy may not have written its target index. Only advance shared throttle state
    // when every strategy accepted the batch.
    if (strategyFailure == null) {
      recordThrottleWrites(groupedEvents);
    }

    // Graph and system-metadata writes are required side effects of the same unit of work. A
    // synchronous failure here must not propagate straight out and bypass the deferred
    // strategyFailure rethrow below, which would clobber a saved RetryableIndexingException — the
    // MAE transports detect retryability by walking the thrown exception's cause chain, so losing a
    // retryable would commit the offset past an unwritten V3 document. Accumulate instead, keeping
    // any retryable failure as the primary cause.
    try {
      // Process each group of events for the same URN together
      for (List<MCLItem> urnEvents : groupedEvents.values()) {
        // Process update events
        List<MCLItem> updateEvents =
            urnEvents.stream()
                .filter(e -> UPDATE_CHANGE_TYPES.contains(e.getMetadataChangeLog().getChangeType()))
                .collect(Collectors.toList());

        if (!updateEvents.isEmpty()) {
          // Update graph indices for each event individually for now
          for (MCLItem event : updateEvents) {
            updateGraph(opContext, event, grouped);
          }

          handleSystemMetadataUpdateChangeEvents(opContext, updateEvents);
        }

        // Process delete events
        List<MCLItem> deleteEvents =
            urnEvents.stream()
                .filter(e -> e.getMetadataChangeLog().getChangeType() == ChangeType.DELETE)
                .collect(Collectors.toList());

        for (MCLItem deleteEvent : deleteEvents) {
          Pair<EntitySpec, AspectSpec> specPair = UpdateIndicesUtil.extractSpecPair(deleteEvent);
          boolean isDeletingKey = UpdateIndicesUtil.isDeletingKey(specPair);

          // graph update first
          updateGraph(opContext, deleteEvent, grouped);

          // system metadata is last for tracing
          handleSystemMetadataDeleteChangeEvent(
              opContext, deleteEvent.getUrn(), specPair, isDeletingKey);
        }
      }
    } catch (RuntimeException sideEffectFailure) {
      strategyFailure = accumulateStrategyFailure(strategyFailure, sideEffectFailure);
    }

    if (strategyFailure != null) {
      throw strategyFailure;
    }
  }

  /**
   * Removes temporary V3 fill guards after the corresponding catch-up window has converged. A
   * cleanup that does not finish throws, so catch-up stays incomplete and the next run retries.
   */
  public void deleteV3Tombstones(@Nonnull OperationContext opContext, @Nonnull String indexName) {
    ESWriteDAO.DeleteByQueryResult result =
        elasticSearchService.deleteV3Tombstones(opContext, indexName);
    if (!result.isSuccess()) {
      throw new IllegalStateException(
          String.format(
              "Removing V3 tombstones from %s did not finish (%d remaining): %s",
              indexName, result.getRemainingDocuments(), result.getFailureReason()));
    }
  }

  /**
   * Writes {@code events} through the strategies that fill new indices from primary storage, and
   * nothing else. A fill rebuilds one search index from state that the graph and system-metadata
   * indices already hold, and the other generation's index is already current, so their writes are
   * skipped. Throttle state is left alone for the same reason.
   */
  public void fillSearchIndices(
      @Nonnull OperationContext opContext, @Nonnull final Collection<MetadataChangeLog> events) {
    LinkedHashMap<Urn, List<MCLItem>> groupedEvents = groupForStrategies(opContext, events).byUrn();
    for (UpdateIndicesStrategy strategy : updateStrategies) {
      if (strategy.isEnabled() && strategy.fillsNewIndicesFromPrimaryStorage()) {
        strategy.processBatch(opContext, groupedEvents, structuredPropertiesHookEnabled);
      }
    }
  }

  /**
   * Events grouped by URN in order, and the previous value of each event that was built without it
   * because a newer version wrote that value (see {@link #buildValidMCLItem}), keyed by identity.
   */
  private record GroupedEvents(
      @Nonnull LinkedHashMap<Urn, List<MCLItem>> byUrn,
      @Nonnull Map<MCLItem, RecordTemplate> unvalidatedPrevious) {}

  /**
   * Converts change logs to {@link MCLItem}s, expands MCL side effects, and groups by URN in order.
   * Cross-routing-key mixing is segregated upstream in the Kafka batch listener (see
   * MCLBatchKafkaListener.consumeBatch), so every call already arrives under a single routing
   * identity and grouping by URN alone is sufficient.
   */
  @Nonnull
  private GroupedEvents groupForStrategies(
      @Nonnull OperationContext opContext, @Nonnull final Collection<MetadataChangeLog> events) {
    // While unknown data is tolerated, skip MCLs with unknown entities/aspects (can happen after
    // rollback when older build encounters rows written by the newer one).
    EntityRegistry entityRegistry = opContext.getAspectRetriever().getEntityRegistry();
    final Map<MCLItem, RecordTemplate> unvalidatedPrevious = new IdentityHashMap<>();
    List<MCLItem> mclItems =
        events.stream()
            .filter(
                event -> {
                  return handling()
                      .admitEvent(
                          UNKNOWN_DATA,
                          entityRegistry,
                          opContext.getMetricUtils(),
                          event.getEntityUrn(),
                          event.hasEntityType() ? event.getEntityType() : null,
                          event.hasAspectName() ? event.getAspectName() : null);
                })
            .map(event -> buildValidMCLItem(opContext, event, unvalidatedPrevious))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    List<MCLItem> sideEffects =
        AspectsBatch.applyMCLSideEffects(mclItems, opContext.getRetrieverContext())
            .collect(Collectors.toList());
    return new GroupedEvents(
        UpdateIndicesUtil.groupEventsByUrn(Stream.concat(mclItems.stream(), sideEffects.stream())),
        unvalidatedPrevious);
  }

  private void updateGraph(
      @Nonnull OperationContext opContext,
      @Nonnull final MCLItem event,
      @Nonnull final GroupedEvents grouped) {
    if (grouped.unvalidatedPrevious().containsKey(event)) {
      updateGraphIndicesService.handleChangeEventWithUnvalidatedPrevious(
          opContext, event.getMetadataChangeLog(), grouped.unvalidatedPrevious().get(event));
    } else {
      updateGraphIndicesService.handleChangeEvent(opContext, event.getMetadataChangeLog());
    }
  }

  /**
   * Builds an MCLItem. An event whose new value fails validation because a newer version wrote it
   * (e.g. read after a rollback) is dropped on its own instead of failing the whole batch, and
   * counted in {@link #INVALID_MCL_SKIPPED_METRIC}. An event whose new value is valid but whose
   * previous value a newer version wrote (e.g. a change away from an enum value only that version
   * defines) is built without the previous value, so its new value is still indexed. The item then
   * carries that previous value, without its unknown references, for the search diff, and the value
   * as read goes in {@code unvalidatedPrevious} for the graph update. Any other invalid payload
   * fails as before.
   */
  @Nullable
  private static MCLItem buildValidMCLItem(
      @Nonnull final OperationContext opContext,
      @Nonnull final MetadataChangeLog event,
      @Nonnull final Map<MCLItem, RecordTemplate> unvalidatedPrevious) {
    try {
      return MCLItemImpl.builder().build(event, opContext.getAspectRetriever());
    } catch (ValidationException e) {
      // Checked first so strict handling does no deserialization and fails as before.
      if (!handling().toleratesUnknownData()) {
        throw e;
      }
      final EntityRegistry registry = opContext.getEntityRegistry();
      final AspectSpec aspectSpec =
          registry.findAspectSpec(event.getEntityType(), event.getAspectName()).orElse(null);
      if (handling()
          .isFromNewerVersion(
              deserializeOrNull(event.hasAspect() ? event.getAspect() : null, aspectSpec),
              event.getSystemMetadata(),
              aspectSpec,
              registry)) {
        opContext
            .getMetricUtils()
            .ifPresent(m -> m.increment(UpdateIndicesService.class, INVALID_MCL_SKIPPED_METRIC, 1));
        UNKNOWN_DATA.skippedBecause(
            Optional.empty(),
            "invalid:" + event.getEntityType() + "/" + event.getAspectName(),
            "its payload from a newer version fails validation here: " + e.getMessage(),
            event.getEntityUrn());
        return null;
      }
      final RecordTemplate previous = deserializeOrNull(event.getPreviousAspectValue(), aspectSpec);
      if (!event.hasPreviousAspectValue()
          || !handling()
              .isFromNewerVersion(
                  previous, event.getPreviousSystemMetadata(), aspectSpec, registry)) {
        throw e;
      }
      // Blame the previous value only when the new one is valid on its own, so an event whose new
      // value is invalid for another reason still fails.
      final MCLItemImpl withoutPrevious = buildWithoutPrevious(opContext, event);
      if (withoutPrevious == null) {
        throw e;
      }
      opContext
          .getMetricUtils()
          .ifPresent(
              m ->
                  m.increment(
                      UpdateIndicesService.class, PREVIOUS_FROM_NEWER_VERSION_OMITTED_METRIC, 1));
      UNKNOWN_DATA.invalid(
          "previous:" + event.getEntityType() + "/" + event.getAspectName(),
          event.getAspectName()
              + "'s previous value, from a newer version (indexing the new value without it): "
              + e.getMessage(),
          event.getEntityUrn());
      // The search diff needs the previous value too: an upsert nulls the fields only it had. It
      // gets its own copy, stripped as a valid previous value is when the item is built, so its
      // transform doesn't meet references to entity types this version doesn't know. The graph
      // update strips its copy only for an upsert.
      final RecordTemplate previousForSearch =
          deserializeOrNull(event.getPreviousAspectValue(), aspectSpec);
      if (previousForSearch != null) {
        handling().stripUnknownReferences(previousForSearch, registry);
      }
      final MCLItem indexed = withoutPrevious.withPreviousRecordTemplate(previousForSearch);
      unvalidatedPrevious.put(indexed, previous);
      return indexed;
    }
  }

  @Nullable
  private static RecordTemplate deserializeOrNull(
      @Nullable final GenericAspect value, @Nullable final AspectSpec aspectSpec) {
    if (value == null || aspectSpec == null) {
      return null;
    }
    try {
      return GenericRecordUtils.deserializeAspect(
          value.getValue(), value.getContentType(), aspectSpec);
    } catch (RuntimeException deserializationFailure) {
      // Unreadable payloads aren't attributed to a newer version.
      return null;
    }
  }

  @Nullable
  private static MCLItemImpl buildWithoutPrevious(
      @Nonnull final OperationContext opContext, @Nonnull final MetadataChangeLog event) {
    final MetadataChangeLog withoutPrevious;
    try {
      withoutPrevious = event.copy();
    } catch (CloneNotSupportedException e) {
      return null;
    }
    withoutPrevious.removePreviousAspectValue();
    withoutPrevious.removePreviousSystemMetadata();
    try {
      return MCLItemImpl.builder().build(withoutPrevious, opContext.getAspectRetriever());
    } catch (RuntimeException invalid) {
      return null;
    }
  }

  private static RuntimeException accumulateStrategyFailure(
      @Nullable RuntimeException currentFailure, @Nonnull RuntimeException newFailure) {
    if (currentFailure == null) {
      return newFailure;
    }
    if (newFailure instanceof RetryableIndexingException
        && !(currentFailure instanceof RetryableIndexingException)) {
      newFailure.addSuppressed(currentFailure);
      return newFailure;
    }
    currentFailure.addSuppressed(newFailure);
    return currentFailure;
  }

  /**
   * Handles system metadata updates for a collection of update change events. This method processes
   * system metadata separately for tracing purposes.
   *
   * @param events the collection of update events
   */
  private void handleSystemMetadataUpdateChangeEvents(
      @Nonnull OperationContext opContext, @Nonnull final Collection<MCLItem> events) {
    if (events.isEmpty()) {
      return;
    }

    // Handle system metadata for non-timeseries aspects
    for (MCLItem event : events) {
      if (!event.getAspectSpec().isTimeseries()) {
        SystemMetadata systemMetadata = event.getSystemMetadata();
        if (systemMetadata != null) {
          systemMetadataService.insert(
              opContext,
              systemMetadata,
              event.getUrn().toString(),
              event.getAspectSpec().getName());

          // If processing status aspect update all aspects for this urn to removed
          if (event.getAspectSpec().getName().equals(Constants.STATUS_ASPECT_NAME)) {
            RecordTemplate aspect = event.getRecordTemplate();
            if (aspect instanceof Status) {
              systemMetadataService.setDocStatus(
                  opContext, event.getUrn().toString(), ((Status) aspect).isRemoved());
            }
          }
        }
      }
    }
  }

  /**
   * Handle the system metadata deletion separately for tracing
   *
   * @param urn delete urn
   * @param specPair entity & aspect spec
   * @param isDeletingKey whether the key aspect is being deleted
   */
  private void handleSystemMetadataDeleteChangeEvent(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      Pair<EntitySpec, AspectSpec> specPair,
      boolean isDeletingKey) {
    if (!specPair.getSecond().isTimeseries()) {
      if (isDeletingKey) {
        // Delete all aspects
        log.debug("Deleting all system metadata for urn: {}", urn);
        systemMetadataService.deleteUrn(opContext, urn.toString());
      } else {
        // Delete all aspects from system metadata service
        log.debug(
            "Deleting system metadata for urn: {}, aspect: {}",
            urn,
            specPair.getSecond().getName());
        systemMetadataService.deleteAspect(
            opContext, urn.toString(), specPair.getSecond().getName());
      }
    }
  }

  /**
   * Updates index mappings for structured property changes. This method delegates to all enabled
   * strategies to ensure both V2 and V3 mappings are updated when needed.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param entitySpec the entity specification
   * @param aspectSpec the aspect specification
   * @param newValue the new aspect value
   * @param oldValue the old aspect value
   */
  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue) {
    for (UpdateIndicesStrategy strategy : updateStrategies) {
      if (strategy.isEnabled()) {
        strategy.updateIndexMappings(opContext, urn, entitySpec, aspectSpec, newValue, oldValue);
      }
    }
  }

  /**
   * After all strategies have processed, record throttle writes for timeseries events that were not
   * throttled. This ensures no strategy's recordWrite races with another strategy's shouldThrottle
   * on the same event within a batch.
   */
  private void recordThrottleWrites(LinkedHashMap<Urn, List<MCLItem>> groupedEvents) {
    if (timeseriesThrottleCache == null || !timeseriesThrottleCache.isEnabled()) {
      return;
    }

    for (List<MCLItem> urnEvents : groupedEvents.values()) {
      for (MCLItem event : urnEvents) {
        if (!UPDATE_CHANGE_TYPES.contains(event.getMetadataChangeLog().getChangeType())) {
          continue;
        }
        if (!event.getAspectSpec().isTimeseries()) {
          continue;
        }

        String entityName = event.getEntitySpec().getName();
        String urnStr = event.getUrn().toString();
        String aspectName = event.getAspectName();
        long eventTimeMs =
            event.getAuditStamp() != null
                ? event.getAuditStamp().getTime()
                : System.currentTimeMillis();

        if (!timeseriesThrottleCache.shouldThrottle(entityName, urnStr, aspectName, eventTimeMs)) {
          timeseriesThrottleCache.recordWrite(urnStr, aspectName, eventTimeMs);
        }
      }
    }
  }

  /**
   * Flushes pending operations on every cluster's bulk processor to ensure all data is written to
   * Elasticsearch. This is particularly important for loadIndices operations where we want to
   * ensure all data is persisted.
   */
  public void flush() {
    try {
      for (ESBulkProcessor bulkProcessor : elasticSearchService.getEsWriteDAO().bulkProcessors()) {
        bulkProcessor.flush();
      }
      log.info("Successfully flushed bulk processors");
    } catch (Exception e) {
      log.error("Failed to flush bulk processors", e);
      throw new RuntimeException("Failed to flush bulk processors", e);
    }
  }

  /**
   * Flushes pending operations on every cluster's bulk processor and waits for every operation
   * captured by the flush boundaries to receive a response or failure callback.
   */
  @Nonnull
  public BulkProcessorFlushResult flushAndAwait() {
    try {
      BulkProcessorFlushResult result = BulkProcessorFlushResult.empty();
      for (ESBulkProcessor bulkProcessor : elasticSearchService.getEsWriteDAO().bulkProcessors()) {
        result = result.plus(bulkProcessor.flushAndAwait());
      }
      log.info("Bulk processor drain completed: {}", result);
      return result;
    } catch (Exception e) {
      log.error("Failed to flush and await bulk processors", e);
      throw new RuntimeException("Failed to flush and await bulk processors", e);
    }
  }

  /** Drain every cluster's bulk processor. Load-indices checkpoints only after this returns. */
  public void flushAndWait() {
    BulkProcessorFlushResult result = flushAndAwait();
    if (result.hasFailures()) {
      throw new IllegalStateException("Bulk processor drain completed with failures: " + result);
    }
  }

  /**
   * When {@code ES_BULK_ACK_AFTER_TRANSFER} is enabled, flush and wait for bulk transfer on every
   * cluster before the caller (MAE) acknowledges Kafka/pgQueue offsets. No-op when the flag is
   * false or when the write path is unavailable (e.g. unit tests with a mocked search service).
   */
  public void flushAndWaitIfConfigured() {
    ESWriteDAO writeDAO = elasticSearchService.getEsWriteDAO();
    if (writeDAO == null) {
      return;
    }
    for (ESBulkProcessor bulkProcessor : writeDAO.bulkProcessors()) {
      if (bulkProcessor == null || !bulkProcessor.isAckAfterTransfer()) {
        continue;
      }
      try {
        Duration timeout = Duration.ofSeconds(bulkProcessor.getAckAfterTransferTimeoutSeconds());
        bulkProcessor.flushAndWait(timeout);
        log.debug("Bulk transfer acknowledged after flush-and-wait");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for bulk transfer", e);
      } catch (TimeoutException e) {
        throw new RuntimeException("Timed out waiting for bulk transfer", e);
      } catch (BulkTransferException e) {
        throw new RuntimeException("Bulk transfer failed before offset ack", e);
      }
    }
  }

  /** Whether a V3 update strategy is registered (i.e. V3 writes are active in this process). */
  public boolean hasV3UpdateStrategy() {
    return updateStrategies.stream().anyMatch(UpdateIndicesV3Strategy.class::isInstance);
  }

  /**
   * Names of the entity indices whose owning strategy fills new indices from primary storage. See
   * {@link UpdateIndicesStrategy#fillsNewIndicesFromPrimaryStorage()}.
   */
  @Nonnull
  public Set<String> indicesFilledFromPrimaryStorage(@Nonnull OperationContext opContext) {
    return updateStrategies.stream()
        .filter(UpdateIndicesStrategy::isEnabled)
        .filter(UpdateIndicesStrategy::fillsNewIndicesFromPrimaryStorage)
        .flatMap(strategy -> strategy.getIndexMappings(opContext).stream())
        .map(MappingsBuilder.IndexMapping::getIndexName)
        .collect(Collectors.toSet());
  }

  /**
   * Returns and resets the V3 write totals consumed by intermediate strategy drains, so run-level
   * accounting (e.g. LoadIndices) can add them to the final flush result.
   */
  @Nonnull
  public UpdateIndicesV3Strategy.DrainedWriteStats consumeDrainedV3WriteStats() {
    long successfulV3Writes = 0;
    long staleVersionRejected = 0;
    for (UpdateIndicesStrategy strategy : updateStrategies) {
      if (strategy instanceof UpdateIndicesV3Strategy v3Strategy) {
        UpdateIndicesV3Strategy.DrainedWriteStats stats = v3Strategy.consumeDrainedWriteStats();
        successfulV3Writes += stats.successfulV3Writes();
        staleVersionRejected += stats.staleVersionRejected();
      }
    }
    return new UpdateIndicesV3Strategy.DrainedWriteStats(successfulV3Writes, staleVersionRejected);
  }
}
