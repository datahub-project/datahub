package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.UpdateIndicesService.UPDATE_CHANGE_TYPES;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.entity.EntityService;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
 * <p>Periodically polls the persisted upgrade state to detect indices marked as {@code
 * DUAL_WRITE_DISABLED} and stops writing to them. The poller shuts down once all targets are
 * removed.
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
  private final ConcurrentHashMap<String, String> oldIndexTargets;

  /**
   * Tracks whether dual-write start time has been recorded for each index. Key is old index name.
   */
  private final ConcurrentHashMap<String, AtomicBoolean> dualWriteStartTimeRecorded;

  @Nullable private final DualWriteStartTimeCallback dualWriteStartTimeCallback;
  @Nullable private final ScheduledExecutorService statePoller;

  private static final long DEFAULT_POLL_INTERVAL_SECONDS = 300;

  /** Callback interface for persisting dual-write start time to upgrade result. */
  @FunctionalInterface
  public interface DualWriteStartTimeCallback {
    void onDualWriteStarted(String entityName, long startTimeMillis);
  }

  public UpdateIndicesUpgradeStrategy(
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull Map<String, String> oldIndexTargets,
      @Nullable DualWriteStartTimeCallback dualWriteStartTimeCallback,
      @Nullable OperationContext opContext,
      @Nullable EntityService<?> entityService,
      @Nullable Urn upgradeIdUrn,
      long pollIntervalSeconds) {
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentTransformer = searchDocumentTransformer;
    this.oldIndexTargets = new ConcurrentHashMap<>(oldIndexTargets);
    this.dualWriteStartTimeRecorded = new ConcurrentHashMap<>();
    this.dualWriteStartTimeCallback = dualWriteStartTimeCallback;

    if (!oldIndexTargets.isEmpty()) {
      log.info(
          "UpdateIndicesUpgradeStrategy initialized with {} old index targets for rollback dual-write: {}",
          oldIndexTargets.size(),
          oldIndexTargets);

      if (opContext != null && entityService != null && upgradeIdUrn != null) {
        statePoller =
            Executors.newSingleThreadScheduledExecutor(
                r -> {
                  Thread t = new Thread(r, "incremental-reindex-state-poller");
                  t.setDaemon(true);
                  return t;
                });
        long interval =
            pollIntervalSeconds > 0 ? pollIntervalSeconds : DEFAULT_POLL_INTERVAL_SECONDS;
        statePoller.scheduleAtFixedRate(
            () -> pollForSwappedIndices(opContext, entityService, upgradeIdUrn),
            interval,
            interval,
            TimeUnit.SECONDS);
      } else {
        statePoller = null;
      }
    } else {
      log.info("UpdateIndicesUpgradeStrategy initialized with no active upgrade targets");
      statePoller = null;
    }
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
    String oldIndex = oldIndexTargets.get(entityName);
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

      elasticSearchService.upsertDocumentByIndexName(oldIndex, document, docId);

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
    String oldIndex = oldIndexTargets.get(entityName);
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
      elasticSearchService.deleteDocumentByIndexName(oldIndex, docId);

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
    String removed = oldIndexTargets.remove(entityName);
    if (removed != null) {
      log.info(
          "Removed rollback dual-write target for entity '{}' (was '{}')", entityName, removed);
      if (oldIndexTargets.isEmpty()) {
        shutdownPoller();
      }
    }
  }

  // Package-private for testing
  void pollForSwappedIndices(
      OperationContext opContext, EntityService<?> entityService, Urn upgradeIdUrn) {
    try {
      Optional<DataHubUpgradeResult> result =
          getUpgradeResult(opContext, entityService, upgradeIdUrn);
      if (result.isEmpty() || result.get().getResult() == null) {
        return;
      }

      Map<String, Map<String, String>> allStates =
          IncrementalReindexState.getAllIndexStates(result.get().getResult());
      IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();

      Set<String> swappedEntities =
          allStates.entrySet().stream()
              .filter(
                  e ->
                      IncrementalReindexState.Status.DUAL_WRITE_DISABLED
                          .name()
                          .equals(e.getValue().get(IncrementalReindexState.STATUS)))
              .map(e -> indexConvention.getEntityName(e.getKey()))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toSet());

      Set<String> toRemove =
          oldIndexTargets.keySet().stream()
              .filter(swappedEntities::contains)
              .collect(Collectors.toSet());

      for (String entityName : toRemove) {
        removeTarget(entityName);
      }
    } catch (Exception e) {
      log.warn("Failed to poll for swapped indices: {}", e.getMessage());
    }
  }

  private Optional<DataHubUpgradeResult> getUpgradeResult(
      OperationContext opContext, EntityService<?> entityService, Urn upgradeIdUrn) {
    try {
      EntityResponse response =
          entityService.getEntityV2(
              opContext,
              upgradeIdUrn.getEntityType(),
              upgradeIdUrn,
              Set.of("dataHubUpgradeResult"));
      if (response != null && response.getAspects().containsKey("dataHubUpgradeResult")) {
        return Optional.of(
            new DataHubUpgradeResult(
                response.getAspects().get("dataHubUpgradeResult").getValue().data()));
      }
    } catch (Exception e) {
      log.debug("Could not fetch upgrade result for {}: {}", upgradeIdUrn, e.getMessage());
    }
    return Optional.empty();
  }

  private void shutdownPoller() {
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
}
