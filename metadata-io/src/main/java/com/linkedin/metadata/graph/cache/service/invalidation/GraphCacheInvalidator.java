package com.linkedin.metadata.graph.cache.service.invalidation;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.linkedin.metadata.cache.visibility.IndexVisibilityEstimator;
import com.linkedin.metadata.cache.visibility.SourceIndexKind;
import com.linkedin.metadata.cache.visibility.VisibilityMode;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.InvalidationVisibility;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationBatch;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationEntry;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.config.EntityGraphSources;
import com.linkedin.metadata.graph.cache.service.invalidation.SyncInvalidationPolicy.InvalidationAction;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashSet;
import java.util.OptionalLong;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphCacheInvalidator {

  private final EntityGraphRegistry registry;
  private final EntityGraphDistributedStore distributedStore;
  private final EntityGraphLocalViewCache localViews;
  private final OperationContext systemOperationContext;
  private final IndexVisibilityEstimator visibilityEstimator;

  public GraphCacheInvalidator(
      @Nonnull EntityGraphRegistry registry,
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphLocalViewCache localViews,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull IndexVisibilityEstimator visibilityEstimator) {
    this.registry = registry;
    this.distributedStore = distributedStore;
    this.localViews = localViews;
    this.systemOperationContext = systemOperationContext;
    this.visibilityEstimator = visibilityEstimator;
  }

  /** State of one invalidation pass over a batch. */
  private static final class Pass {
    final Set<String> handledGraphs = new LinkedHashSet<>();
    final IndexVisibilityEstimator.Session visibility;
    final boolean allowDeferral;

    Pass(IndexVisibilityEstimator.Session visibility, boolean allowDeferral) {
      this.visibility = visibility;
      this.allowDeferral = allowDeferral;
    }
  }

  /**
   * Invalidates after a synchronous write. A graph that configures {@code invalidation.visibility}
   * keeps its snapshots and records when the write becomes searchable in the source index; every
   * other graph is dropped now.
   */
  public void invalidateOnSyncBatch(@Nonnull SyncGraphInvalidationBatch batch) {
    invalidate(batch, true);
  }

  /** Drops affected graphs now, ignoring any configured visibility deferral. */
  public void invalidateImmediately(@Nonnull SyncGraphInvalidationBatch batch) {
    invalidate(batch, false);
  }

  private void invalidate(@Nonnull SyncGraphInvalidationBatch batch, boolean allowDeferral) {
    if (batch.isEmpty()) {
      return;
    }
    Pass pass = new Pass(visibilityEstimator.newSession(), allowDeferral);
    for (SyncGraphInvalidationEntry create : batch.getCreates()) {
      handleCreate(create, pass);
    }
    for (SyncGraphInvalidationEntry delete : batch.getDeletes()) {
      handleDelete(delete, pass);
    }
    for (SyncGraphInvalidationEntry update : batch.getUpdates()) {
      handleUpdate(update, pass);
    }
  }

  void handleCreate(@Nonnull SyncGraphInvalidationEntry entry, @Nonnull Pass pass) {
    if (entry.getAspectName() == null) {
      return;
    }
    for (String graphId : graphIdsForEntry(entry)) {
      EntityGraphDefinition definition = registry.getDefinition(graphId);
      if (definition == null
          || registry.getRelationshipTypesForAspect(graphId, entry.getAspectName()).isEmpty()) {
        continue;
      }
      CacheStatus status = primaryGraphCacheStatus(definition);
      InvalidationAction action =
          SyncInvalidationPolicy.forCreate(status, definition.getScope().getMode());
      if ((action == InvalidationAction.DROP_GRAPH || action == InvalidationAction.DROP_PARTIAL)
          && pass.handledGraphs.add(graphId)) {
        invalidateGraph(definition, entry, status, pass);
      }
    }
  }

  void handleUpdate(@Nonnull SyncGraphInvalidationEntry entry, @Nonnull Pass pass) {
    if (entry.getAspectName() == null) {
      return;
    }
    if (STATUS_ASPECT_NAME.equals(entry.getAspectName())) {
      removeEntityFromGraphs(entry, pass);
      return;
    }
    for (String graphId : graphIdsForEntry(entry)) {
      EntityGraphDefinition definition = registry.getDefinition(graphId);
      if (definition == null
          || registry.getRelationshipTypesForAspect(graphId, entry.getAspectName()).isEmpty()) {
        continue;
      }
      CacheStatus status = primaryGraphCacheStatus(definition);
      InvalidationAction action =
          SyncInvalidationPolicy.forUpdate(
              status, definition.getScope().getMode(), entry.getAspectName());
      if (action == InvalidationAction.SURGICAL_REMOVE) {
        removeEntityFromGraphs(entry, pass);
        continue;
      }
      if ((action == InvalidationAction.DROP_GRAPH || action == InvalidationAction.DROP_PARTIAL)
          && pass.handledGraphs.add(graphId)) {
        invalidateGraph(definition, entry, status, pass);
      }
    }
  }

  void handleDelete(@Nonnull SyncGraphInvalidationEntry entry, @Nonnull Pass pass) {
    removeEntityFromGraphs(entry, pass);
  }

  void removeEntityFromGraphs(@Nonnull SyncGraphInvalidationEntry entry, @Nonnull Pass pass) {
    for (String graphId : graphIdsForEntry(entry)) {
      EntityGraphDefinition definition = registry.getDefinition(graphId);
      if (definition == null) {
        continue;
      }
      CacheStatus status = primaryGraphCacheStatus(definition);
      InvalidationAction action =
          SyncInvalidationPolicy.forDelete(status, definition.getScope().getMode());
      switch (action) {
        case NO_OP -> {
          // Nothing to invalidate for this graph in its current status.
        }
        case DROP_GRAPH, DROP_PARTIAL -> {
          // A batch of deletes would otherwise record a watermark per entity.
          if (pass.handledGraphs.add(graphId)) {
            invalidateGraph(definition, entry, status, pass);
          }
        }
        case SURGICAL_REMOVE -> {
          boolean changed =
              distributedStore.removeVertexFromSnapshot(
                  graphId, entry.getEntityUrn(), definition.getBounds().getMaxVertices());
          if (changed) {
            localViews.evictGraph(graphId);
            recordVertexRemoval(graphId);
          }
        }
      }
    }
  }

  @Nonnull
  Set<String> graphIdsForEntityType(@Nonnull String entityType) {
    return registry.getGraphIdsForEntityType(entityType);
  }

  @Nonnull
  Set<String> graphIdsForEntry(@Nonnull SyncGraphInvalidationEntry entry) {
    if (entry.getAspectName() != null) {
      if (STATUS_ASPECT_NAME.equals(entry.getAspectName())) {
        return graphIdsForEntityType(entry.getEntityType());
      }
      return registry.getCandidateGraphIds(entry.getEntityType(), entry.getAspectName());
    }
    return graphIdsForEntityType(entry.getEntityType());
  }

  @Nonnull
  CacheStatus primaryGraphCacheStatus(@Nonnull EntityGraphDefinition definition) {
    if (definition.getScope().getMode() == ScopeMode.FULL) {
      for (GraphSnapshotSource source : EntityGraphSources.supportedBuildSources(definition)) {
        String cacheKey = EntityGraphCacheKeys.fullCacheKey(definition.getGraphId(), source);
        CacheStatus status = distributedStore.getStatus(cacheKey);
        if (status != CacheStatus.ABSENT) {
          return status;
        }
      }
      return CacheStatus.ABSENT;
    }
    return distributedStore.anySnapshotStatusForGraph(definition.getGraphId());
  }

  /**
   * Drops the graph, or defers: a graph that configures invalidation visibility keeps serving its
   * snapshots and records the moment the write is searchable. Readers then treat snapshots built
   * before that moment as superseded. A {@code COOLDOWN} tombstone is dropped now so its retry
   * suppression does not outlive the write.
   */
  private void invalidateGraph(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull SyncGraphInvalidationEntry entry,
      @Nonnull CacheStatus status,
      @Nonnull Pass pass) {
    boolean deferred = pass.allowDeferral && recordVisibilityWatermark(definition, entry, pass);
    if (deferred && status != CacheStatus.COOLDOWN) {
      recordDeferredInvalidation(definition.getGraphId());
      return;
    }
    dropGraphForDefinition(definition);
    recordInvalidation(definition.getGraphId());
  }

  private boolean recordVisibilityWatermark(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull SyncGraphInvalidationEntry entry,
      @Nonnull Pass pass) {
    if (!definition.usesInvalidationVisibility()) {
      return false;
    }
    SourceIndexKind kind = sourceIndexKind(definition);
    if (kind == null) {
      return false;
    }
    VisibilityMode mode =
        definition.getInvalidationVisibility() == InvalidationVisibility.FLUSH_AWAIT
            ? VisibilityMode.FLUSH_AWAIT
            : VisibilityMode.SUBMIT_BOUND;
    OptionalLong visibleAt =
        pass.visibility.visibleAtMillis(
            kind, entry.getEntityType(), mode, definition.getInvalidationMarginSeconds());
    if (visibleAt.isEmpty()) {
      return false;
    }
    distributedStore.recordVisibilityWatermark(definition.getGraphId(), visibleAt.getAsLong());
    return true;
  }

  /** Primary-sourced graphs read the write database, which has no visibility lag. */
  private static SourceIndexKind sourceIndexKind(@Nonnull EntityGraphDefinition definition) {
    if (definition.getBuildSource() == GraphSnapshotSource.GRAPH) {
      return SourceIndexKind.GRAPH;
    }
    if (definition.getBuildSource() == GraphSnapshotSource.SEARCH) {
      return SourceIndexKind.ENTITY;
    }
    return null;
  }

  void dropGraphForDefinition(@Nonnull EntityGraphDefinition definition) {
    String graphId = definition.getGraphId();
    if (definition.getScope().getMode() == ScopeMode.FULL) {
      distributedStore.dropGraph(graphId);
    } else {
      distributedStore.dropPartialGraph(graphId);
    }
    localViews.evictGraph(graphId);
  }

  void recordInvalidation(@Nonnull String graphId) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metrics ->
                metrics.incrementMicrometer(
                    "entity.graph.cache.invalidated", 1, "graphId", graphId));
    log.debug("Dropped entity graph cache for graphId {} after sync metadata change", graphId);
  }

  void recordDeferredInvalidation(@Nonnull String graphId) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metrics ->
                metrics.incrementMicrometer(
                    "entity.graph.cache.invalidation_deferred", 1, "graphId", graphId));
    log.debug("Deferred entity graph cache invalidation for graphId {} until visible", graphId);
  }

  void recordVertexRemoval(@Nonnull String graphId) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metrics ->
                metrics.incrementMicrometer(
                    "entity.graph.cache.vertex_removed", 1, "graphId", graphId));
    log.debug("Removed vertex from entity graph cache for graphId {}", graphId);
  }
}
