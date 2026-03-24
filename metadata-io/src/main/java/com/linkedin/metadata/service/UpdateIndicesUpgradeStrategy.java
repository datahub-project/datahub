package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.UpdateIndicesService.UPDATE_CHANGE_TYPES;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Update indices strategy for the incremental reindex upgrade. Dual-writes search documents to
 * 'next' indices that were created during Phase 1 (blocking system update).
 *
 * <p>This strategy reads the Phase 1 upgrade result to discover which indices have pending 'next'
 * versions, and writes to them alongside the primary index strategies (V2/V3). It records the
 * dual-write start time on the first successful write for each index, which Phase 2's catch-up step
 * uses to determine its query window.
 *
 * <p>This strategy is a no-op when no incremental reindex is in progress.
 */
@Slf4j
public class UpdateIndicesUpgradeStrategy implements UpdateIndicesStrategy {

  private final ElasticSearchService elasticSearchService;
  private final SearchDocumentTransformer searchDocumentTransformer;

  /**
   * Map of entity name → next index physical name. Populated on startup from Phase 1 upgrade
   * result. Entries are removed when alias swap completes.
   */
  private final ConcurrentHashMap<String, String> nextIndexTargets;

  /**
   * Tracks whether dual-write start time has been recorded for each index. Key is next index name.
   */
  private final ConcurrentHashMap<String, AtomicBoolean> dualWriteStartTimeRecorded;

  /**
   * Callback to persist dual-write start time. Injected by the factory so the strategy doesn't need
   * direct access to EntityService.
   */
  @Nullable private final DualWriteStartTimeCallback dualWriteStartTimeCallback;

  /** Callback interface for persisting dual-write start time to upgrade result. */
  @FunctionalInterface
  public interface DualWriteStartTimeCallback {
    void onDualWriteStarted(String indexName, long startTimeMillis);
  }

  public UpdateIndicesUpgradeStrategy(
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull Map<String, String> nextIndexTargets,
      @Nullable DualWriteStartTimeCallback dualWriteStartTimeCallback) {
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentTransformer = searchDocumentTransformer;
    this.nextIndexTargets = new ConcurrentHashMap<>(nextIndexTargets);
    this.dualWriteStartTimeRecorded = new ConcurrentHashMap<>();
    this.dualWriteStartTimeCallback = dualWriteStartTimeCallback;

    if (!nextIndexTargets.isEmpty()) {
      log.info(
          "UpdateIndicesUpgradeStrategy initialized with {} next index targets: {}",
          nextIndexTargets.size(),
          nextIndexTargets);
    } else {
      log.info("UpdateIndicesUpgradeStrategy initialized with no active upgrade targets");
    }
  }

  @Override
  public boolean isEnabled() {
    return !nextIndexTargets.isEmpty();
  }

  @Override
  public void processBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, List<MCLItem>> groupedEvents,
      boolean structuredPropertiesHookEnabled) {

    if (nextIndexTargets.isEmpty()) {
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

  private void processUpdateEvent(
      @Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    String entityName = event.getEntitySpec().getName();
    String nextIndex = nextIndexTargets.get(entityName);
    if (nextIndex == null) {
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

      elasticSearchService.upsertDocumentByIndexName(nextIndex, document, docId);

      recordDualWriteStartIfNeeded(entityName, nextIndex);

      log.debug(
          "Upgrade dual-write: upserted doc to '{}' for entity '{}', urn '{}'",
          nextIndex,
          entityName,
          event.getUrn());
    } catch (Exception e) {
      log.error(
          "Upgrade dual-write failed for entity '{}', urn '{}', next index '{}': {}",
          entityName,
          event.getUrn(),
          nextIndex,
          e.getMessage(),
          e);
    }
  }

  private void processDeleteEvent(
      @Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    String entityName = event.getEntitySpec().getName();
    String nextIndex = nextIndexTargets.get(entityName);
    if (nextIndex == null) {
      return;
    }

    boolean isDeletingKey =
        event.getEntitySpec().getKeyAspectSpec().getName().equals(event.getAspectName());
    if (!isDeletingKey) {
      // Non-key aspect deletion: re-transform remaining aspects and upsert
      processUpdateEvent(opContext, event);
      return;
    }

    try {
      String docId =
          opContext.getSearchContext().getIndexConvention().getEntityDocumentId(event.getUrn());
      elasticSearchService.deleteDocumentByIndexName(nextIndex, docId);

      log.debug(
          "Upgrade dual-write: deleted doc from '{}' for entity '{}', urn '{}'",
          nextIndex,
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

  private void recordDualWriteStartIfNeeded(String entityName, String nextIndex) {
    AtomicBoolean recorded =
        dualWriteStartTimeRecorded.computeIfAbsent(nextIndex, k -> new AtomicBoolean(false));
    if (recorded.compareAndSet(false, true) && dualWriteStartTimeCallback != null) {
      long now = System.currentTimeMillis();
      try {
        dualWriteStartTimeCallback.onDualWriteStarted(entityName, now);
        log.info(
            "Recorded dual-write start time for index '{}' (entity '{}'): {}",
            nextIndex,
            entityName,
            now);
      } catch (Exception e) {
        log.error("Failed to persist dual-write start time for index '{}': {}", nextIndex, e.getMessage());
        recorded.set(false); // allow retry
      }
    }
  }

  /** Remove a next index target after alias swap completes. */
  public void removeTarget(String entityName) {
    String removed = nextIndexTargets.remove(entityName);
    if (removed != null) {
      log.info("Removed upgrade dual-write target for entity '{}' (was '{}')", entityName, removed);
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