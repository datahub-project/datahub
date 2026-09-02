package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.UpdateIndicesService.UPDATE_CHANGE_TYPES;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Update indices strategy for rollback dual-write during incremental reindex. After Phase 1 swaps
 * the alias to the next index, this strategy dual-writes search documents to the OLD backing index
 * so that rollback to the previous code version remains possible.
 *
 * <p>This strategy reads the Phase 1 upgrade result to discover which indices have old backing
 * index names recorded, and writes to them alongside the primary index strategies (V2/V3). It
 * records the dual-write start time on the first successful write for each index, which Phase 2's
 * catch-up step uses to determine its query window.
 *
 * <p>Periodically reconciles against the persisted upgrade state: it picks up indices that have
 * completed Phase 1 and drops those marked {@code DUAL_WRITE_DISABLED}. Because it adds as well as
 * removes, a state read that fails at startup — a network call in restli deployments — recovers on
 * a later poll rather than leaving dual-write off for the process lifetime. The poller shuts down
 * once state has been read successfully and no targets remain.
 *
 * <p>This strategy is a no-op when no incremental reindex is in progress or when rollback
 * dual-write is not enabled.
 */
@Slf4j
public class UpdateIndicesUpgradeStrategy implements UpdateIndicesStrategy {

  private final ElasticSearchService elasticSearchService;
  private final SearchDocumentTransformer searchDocumentTransformer;

  /**
   * Map of entity name → old backing index physical name. Populated on startup from Phase 1 upgrade
   * result. Entries are removed when dual-write is disabled.
   */
  @VisibleForTesting @Getter private final ConcurrentHashMap<String, String> oldIndexTargets;

  /**
   * Tracks whether dual-write start time has been recorded for each index. Key is old index name.
   */
  private final ConcurrentHashMap<String, AtomicBoolean> dualWriteStartTimeRecorded;

  @Nullable private final DualWriteStartTimeCallback dualWriteStartTimeCallback;
  @Nullable private final ScheduledExecutorService statePoller;

  /**
   * Whether a read of the persisted upgrade state has ever succeeded. Distinguishes "no targets
   * because the upgrade is finished" from "no targets because we could not read yet" — only the
   * former should stop the poller.
   */
  private volatile boolean upgradeStateObserved = false;

  private static final long DEFAULT_POLL_INTERVAL_SECONDS = 300;

  /**
   * Callback interface for persisting dual-write start time to upgrade result.
   *
   * <p>Implementations must throw if the write did not land. {@link #recordDualWriteStartIfNeeded}
   * treats a normal return as durably persisted and never asks again, so swallowing a failure here
   * loses the start time permanently and leaves Phase 2's catch-up step without a query window.
   */
  @FunctionalInterface
  public interface DualWriteStartTimeCallback {
    void onDualWriteStarted(String entityName, long startTimeMillis) throws Exception;
  }

  public UpdateIndicesUpgradeStrategy(
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull Map<String, String> oldIndexTargets,
      @Nullable DualWriteStartTimeCallback dualWriteStartTimeCallback,
      @Nullable OperationContext opContext,
      @Nullable DataHubUpgradeResultStore upgradeResultStore,
      @Nullable Urn upgradeIdUrn,
      long pollIntervalSeconds) {
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentTransformer = searchDocumentTransformer;
    // Keys are normalised because the two sides disagree on case: the map is built from
    // IndexConvention.getEntityName(), which derives names from the lowercased index
    // ("aiagentindex_v2" -> "aiagent"), while lookups use EntitySpec.getName(), which is the
    // entity-registry name ("aiAgent"). Without this every entity whose registered name is not
    // all-lowercase silently never dual-writes.
    this.oldIndexTargets = new ConcurrentHashMap<>();
    oldIndexTargets.forEach(
        (entityName, index) -> this.oldIndexTargets.put(key(entityName), index));
    this.dualWriteStartTimeRecorded = new ConcurrentHashMap<>();
    this.dualWriteStartTimeCallback = dualWriteStartTimeCallback;

    if (opContext != null && upgradeResultStore != null && upgradeIdUrn != null) {
      // Seed synchronously so the first MCL batch already dual-writes, then poll. The poller starts
      // regardless of the outcome: if this read failed, it is the thing that recovers.
      reconcileTargets(opContext, upgradeResultStore, upgradeIdUrn);

      statePoller =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "incremental-reindex-state-poller");
                t.setDaemon(true);
                return t;
              });
      long interval = pollIntervalSeconds > 0 ? pollIntervalSeconds : DEFAULT_POLL_INTERVAL_SECONDS;
      statePoller.scheduleAtFixedRate(
          () -> reconcileTargets(opContext, upgradeResultStore, upgradeIdUrn),
          interval,
          interval,
          TimeUnit.SECONDS);
    } else {
      statePoller = null;
    }

    log.info(
        "UpdateIndicesUpgradeStrategy initialized with {} old index target(s) for rollback dual-write: {}",
        this.oldIndexTargets.size(),
        this.oldIndexTargets);
  }

  @Override
  public boolean isEnabled() {
    return !oldIndexTargets.isEmpty();
  }

  @Override
  public void processBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, List<MCLItem>> groupedEvents,
      boolean structuredPropertiesHookEnabled) {

    if (oldIndexTargets.isEmpty()) {
      return;
    }

    for (List<MCLItem> urnEvents : groupedEvents.values()) {
      for (MCLItem event : urnEvents) {
        ChangeType changeType = event.getMetadataChangeLog().getChangeType();
        if (UPDATE_CHANGE_TYPES.contains(changeType)) {
          processUpdateEvent(opContext, event);
        } else if (changeType == ChangeType.DELETE) {
          processDeleteEvent(opContext, event);
        }
      }
    }
  }

  private void processUpdateEvent(@Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    String entityName = event.getEntitySpec().getName();
    String oldIndex = oldIndexTargets.get(key(entityName));
    if (oldIndex == null) {
      return;
    }

    try {
      Optional<ObjectNode> searchDocument =
          searchDocumentTransformer.transformAspect(
              opContext,
              event.getUrn(),
              event.getRecordTemplate(),
              event.getAspectSpec(),
              false,
              event.getAuditStamp());

      if (searchDocument.isEmpty()) {
        return;
      }

      String docId =
          opContext.getSearchContext().getIndexConvention().getEntityDocumentId(event.getUrn());
      String document = searchDocument.get().toString();

      elasticSearchService.upsertDocumentByIndexName(opContext, oldIndex, document, docId);

      recordDualWriteStartIfNeeded(entityName, oldIndex);

      log.debug(
          "Rollback dual-write: upserted doc to '{}' for entity '{}', urn '{}'",
          oldIndex,
          entityName,
          event.getUrn());
    } catch (Exception e) {
      log.error(
          "Rollback dual-write failed for entity '{}', urn '{}', old index '{}': {}",
          entityName,
          event.getUrn(),
          oldIndex,
          e.getMessage(),
          e);
    }
  }

  private void processDeleteEvent(@Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    String entityName = event.getEntitySpec().getName();
    String oldIndex = oldIndexTargets.get(key(entityName));
    if (oldIndex == null) {
      return;
    }

    boolean isDeletingKey =
        event.getEntitySpec().getKeyAspectSpec().getName().equals(event.getAspectName());
    if (!isDeletingKey) {
      processUpdateEvent(opContext, event);
      return;
    }

    try {
      String docId =
          opContext.getSearchContext().getIndexConvention().getEntityDocumentId(event.getUrn());
      elasticSearchService.deleteDocumentByIndexName(opContext, oldIndex, docId);

      log.debug(
          "Rollback dual-write: deleted doc from '{}' for entity '{}', urn '{}'",
          oldIndex,
          entityName,
          event.getUrn());
    } catch (Exception e) {
      log.error(
          "Upgrade dual-write delete failed for entity '{}', urn '{}': {}",
          entityName,
          event.getUrn(),
          e.getMessage(),
          e);
    }
  }

  private void recordDualWriteStartIfNeeded(String entityName, String oldIndex) {
    AtomicBoolean recorded =
        dualWriteStartTimeRecorded.computeIfAbsent(oldIndex, k -> new AtomicBoolean(false));
    if (recorded.compareAndSet(false, true) && dualWriteStartTimeCallback != null) {
      long now = System.currentTimeMillis();
      try {
        dualWriteStartTimeCallback.onDualWriteStarted(entityName, now);
        log.info(
            "Recorded dual-write start time for index '{}' (entity '{}'): {}",
            oldIndex,
            entityName,
            now);
      } catch (Exception e) {
        log.error(
            "Failed to persist dual-write start time for index '{}': {}", oldIndex, e.getMessage());
        recorded.set(false); // allow retry
      }
    }
  }

  /**
   * Remove an old index target after dual-write is disabled. Shuts down poller when no targets
   * remain.
   */
  public void removeTarget(String entityName) {
    String removed = oldIndexTargets.remove(key(entityName));
    if (removed != null) {
      log.info(
          "Removed rollback dual-write target for entity '{}' (was '{}')", entityName, removed);
      maybeShutdownPoller();
    }
  }

  /**
   * Reconciles the live target map against persisted Phase 1 state: adds indices that completed
   * Phase 1 with an old backing index recorded, and drops those whose dual-write has been disabled.
   *
   * <p>Adding — not just removing — is what makes a failed initial load recoverable. The read is a
   * network call in restli deployments, and a GMS that is not serving yet at consumer startup used
   * to leave the target map empty for the process lifetime, silently disabling dual-write and
   * letting the old backing index go stale.
   */
  // Package-private for testing
  void reconcileTargets(
      OperationContext opContext, DataHubUpgradeResultStore upgradeResultStore, Urn upgradeIdUrn) {
    final DataHubUpgradeResult result;
    try {
      result =
          DataHubUpgradeResultConditionalPersist.fromEnveloped(
              upgradeResultStore.readLatest(opContext, upgradeIdUrn));
    } catch (Exception e) {
      // Deliberately not treated as "no state": keep whatever targets we already hold, because
      // dropping them would silently stop protecting the old backing index.
      log.warn(
          "Could not read incremental reindex state for {}, keeping {} dual-write target(s): {}",
          upgradeIdUrn,
          oldIndexTargets.size(),
          e.getMessage());
      return;
    }

    if (result == null || result.getResult() == null) {
      log.debug("No incremental reindex state recorded for {}", upgradeIdUrn);
      return;
    }

    final Map<String, Map<String, String>> allStates =
        IncrementalReindexState.getAllIndexStates(result.getResult());
    final IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();

    for (Map.Entry<String, Map<String, String>> entry : allStates.entrySet()) {
      final String indexName = entry.getKey();
      final Map<String, String> indexState = entry.getValue();
      final Optional<String> entityName = indexConvention.getEntityName(opContext, indexName);
      if (entityName.isEmpty()) {
        continue;
      }

      final String status = indexState.get(IncrementalReindexState.STATUS);
      final String oldBackingIndexName =
          indexState.get(IncrementalReindexState.OLD_BACKING_INDEX_NAME);

      if (IncrementalReindexState.Status.DUAL_WRITE_DISABLED.name().equals(status)) {
        removeTarget(entityName.get());
      } else if (IncrementalReindexState.Status.COMPLETED.name().equals(status)
          && oldBackingIndexName != null
          && !oldBackingIndexName.isEmpty()
          && oldIndexTargets.put(key(entityName.get()), oldBackingIndexName) == null) {
        log.info(
            "Added rollback dual-write target for entity '{}' -> '{}'",
            entityName.get(),
            oldBackingIndexName);
      }
    }

    upgradeStateObserved = true;
    maybeShutdownPoller();
  }

  /**
   * Stops polling once the upgrade is genuinely done. Gated on having actually read state: an empty
   * target map before any successful read means the load has not happened yet, and shutting down
   * there would remove the only chance to recover.
   */
  private void maybeShutdownPoller() {
    if (!upgradeStateObserved || !oldIndexTargets.isEmpty()) {
      return;
    }
    if (statePoller != null && !statePoller.isShutdown()) {
      log.info("All dual-write targets removed, shutting down state poller");
      statePoller.shutdown();
    }
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext) {
    return Collections.emptyList();
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    return Collections.emptyList();
  }

  @Override
  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue) {
    // No-op: next indices were created with target mappings during Phase 1
  }

  /** Entity-name key normaliser — see the constructor for why this is needed. */
  private static String key(String entityName) {
    return entityName == null ? null : entityName.toLowerCase(Locale.ROOT);
  }
}
