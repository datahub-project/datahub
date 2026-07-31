package com.linkedin.metadata.graph.cache.service.invalidation;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

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
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphCacheInvalidator {

  private final EntityGraphRegistry registry;
  private final EntityGraphDistributedStore distributedStore;
  private final EntityGraphLocalViewCache localViews;
  private final OperationContext systemOperationContext;

  public GraphCacheInvalidator(
      @Nonnull EntityGraphRegistry registry,
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphLocalViewCache localViews,
      @Nonnull OperationContext systemOperationContext) {
    this.registry = registry;
    this.distributedStore = distributedStore;
    this.localViews = localViews;
    this.systemOperationContext = systemOperationContext;
  }

  public void invalidateOnSyncBatch(@Nonnull SyncGraphInvalidationBatch batch) {
    if (batch.isEmpty()) {
      return;
    }
    Set<String> droppedGraphs = new LinkedHashSet<>();
    for (SyncGraphInvalidationEntry create : batch.getCreates()) {
      handleCreate(create, droppedGraphs);
    }
    for (SyncGraphInvalidationEntry delete : batch.getDeletes()) {
      handleDelete(delete);
    }
    for (SyncGraphInvalidationEntry update : batch.getUpdates()) {
      handleUpdate(update, droppedGraphs);
    }
  }

  void handleCreate(@Nonnull SyncGraphInvalidationEntry entry, @Nonnull Set<String> droppedGraphs) {
    if (entry.getAspectName() == null) {
      return;
    }
    for (String graphId : graphIdsForEntry(entry)) {
      EntityGraphDefinition definition = registry.getDefinition(graphId);
      if (definition == null
          || registry.getRelationshipTypesForAspect(graphId, entry.getAspectName()).isEmpty()) {
        continue;
      }
      InvalidationAction action =
          SyncInvalidationPolicy.forCreate(
              primaryGraphCacheStatus(definition), definition.getScope().getMode());
      if ((action == InvalidationAction.DROP_GRAPH || action == InvalidationAction.DROP_PARTIAL)
          && droppedGraphs.add(graphId)) {
        dropGraphForDefinition(definition);
        recordInvalidation(graphId);
      }
    }
  }

  void handleUpdate(@Nonnull SyncGraphInvalidationEntry entry, @Nonnull Set<String> droppedGraphs) {
    if (entry.getAspectName() == null) {
      return;
    }
    if (STATUS_ASPECT_NAME.equals(entry.getAspectName())) {
      removeEntityFromGraphs(entry);
      return;
    }
    for (String graphId : graphIdsForEntry(entry)) {
      EntityGraphDefinition definition = registry.getDefinition(graphId);
      if (definition == null
          || registry.getRelationshipTypesForAspect(graphId, entry.getAspectName()).isEmpty()) {
        continue;
      }
      InvalidationAction action =
          SyncInvalidationPolicy.forUpdate(
              primaryGraphCacheStatus(definition),
              definition.getScope().getMode(),
              entry.getAspectName());
      if (action == InvalidationAction.SURGICAL_REMOVE) {
        removeEntityFromGraphs(entry);
        continue;
      }
      if ((action == InvalidationAction.DROP_GRAPH || action == InvalidationAction.DROP_PARTIAL)
          && droppedGraphs.add(graphId)) {
        dropGraphForDefinition(definition);
        recordInvalidation(graphId);
      }
    }
  }

  void handleDelete(@Nonnull SyncGraphInvalidationEntry entry) {
    removeEntityFromGraphs(entry);
  }

  void removeEntityFromGraphs(@Nonnull SyncGraphInvalidationEntry entry) {
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
          dropGraphForDefinition(definition);
          recordInvalidation(graphId);
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
