package com.linkedin.metadata.graph.cache.service.rebuild;

import static com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator.staleBuildingMillis;
import static com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator.usesBackgroundRebuild;
import static com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext.viewFromEdges;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphBuildFailure;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphSources;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphCacheRebuilder {
  private final EntityGraphDistributedStore distributedStore;
  private final EntityGraphLocalViewCache localViews;
  private final EntityGraphSnapshotBuilder snapshotBuilder;
  private final OperationContext systemOperationContext;
  private final ExecutorService rebuildExecutor;
  private final SnapshotFreshnessEvaluator freshnessEvaluator;
  private final GraphScopeRebuildStrategy fullRebuildStrategy;
  private final GraphScopeRebuildStrategy partialRebuildStrategy;

  public GraphCacheRebuilder(
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphLocalViewCache localViews,
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull ExecutorService rebuildExecutor,
      @Nonnull SnapshotFreshnessEvaluator freshnessEvaluator) {
    this(
        distributedStore,
        localViews,
        snapshotBuilder,
        systemOperationContext,
        rebuildExecutor,
        freshnessEvaluator,
        new FullGraphRebuildStrategy(snapshotBuilder, systemOperationContext),
        new PartialGraphRebuildStrategy(distributedStore, snapshotBuilder, systemOperationContext));
  }

  GraphCacheRebuilder(
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphLocalViewCache localViews,
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull ExecutorService rebuildExecutor,
      @Nonnull SnapshotFreshnessEvaluator freshnessEvaluator,
      @Nonnull GraphScopeRebuildStrategy fullRebuildStrategy,
      @Nonnull GraphScopeRebuildStrategy partialRebuildStrategy) {
    this.distributedStore = distributedStore;
    this.localViews = localViews;
    this.snapshotBuilder = snapshotBuilder;
    this.systemOperationContext = systemOperationContext;
    this.rebuildExecutor = rebuildExecutor;
    this.freshnessEvaluator = freshnessEvaluator;
    this.fullRebuildStrategy = fullRebuildStrategy;
    this.partialRebuildStrategy = partialRebuildStrategy;
  }

  public void ensureFreshKey(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String cacheKey,
      @Nullable Collection<String> seeds,
      @Nonnull TraversalDirection direction) {
    if (definition.getPopulationStrategy() == PopulationStrategy.SCHEDULED) {
      return;
    }
    CacheStatus status = distributedStore.getStatus(cacheKey);
    if (freshnessEvaluator.shouldSkipRebuild(status, cacheKey, definition)) {
      return;
    }
    if (status == CacheStatus.ACTIVE) {
      EntityGraphSnapshot snapshot = distributedStore.getSnapshot(cacheKey);
      if (snapshot != null
          && !SnapshotFreshnessEvaluator.isStale(snapshot, definition)
          && SnapshotFreshnessEvaluator.coverageSatisfies(
              definition, snapshot.getTraversalCoverage(), direction)) {
        return;
      }
    }
    if (!distributedStore.tryClaimRebuild(cacheKey, staleBuildingMillis(definition))) {
      return;
    }
    if (usesBackgroundRebuild(definition)) {
      enqueueBackgroundRebuild(definition, source, cacheKey, seeds, direction);
      return;
    }
    executeRebuild(definition, source, seeds, cacheKey, direction);
  }

  public void executeRebuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nullable Collection<String> seeds,
      @Nullable String publishKeyHint,
      @Nonnull TraversalDirection direction) {
    @Nullable
    String rebuildClaimKey =
        PartialGraphRebuildStrategy.rebuildClaimKey(
            definition.getGraphId(), source, publishKeyHint, seeds);
    long invalidationGenAtStart =
        distributedStore.getInvalidationGeneration(definition.getGraphId());
    try {
      BuildResult result;
      if (definition.getScope().getMode() == ScopeMode.PARTIAL) {
        if (seeds == null || seeds.isEmpty()) {
          markFailedBuild(definition, source, publishKeyHint, seeds, "partial_requires_seeds");
          return;
        }
        result =
            partialRebuildStrategy.executeRebuild(
                definition, source, seeds, publishKeyHint, direction);
      } else {
        result =
            fullRebuildStrategy.executeRebuild(
                definition, source, seeds, publishKeyHint, direction);
      }

      if (result.getStatus() != CacheStatus.ACTIVE) {
        markFailedBuild(definition, source, publishKeyHint, seeds, result.getFailureReason());
        return;
      }

      publishActiveSnapshot(
          definition, result.getSnapshot(), seeds, rebuildClaimKey, invalidationGenAtStart);
    } catch (RuntimeException e) {
      if (rebuildClaimKey != null) {
        handleRebuildException(definition, source, publishKeyHint, seeds, e);
      }
      throw e;
    }
  }

  public void warmCacheIfMissingOrStale(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull EntityGraphSnapshot snapshot,
      @Nonnull TraversalDirection direction,
      @Nullable Collection<String> seeds) {
    String cacheKey = snapshot.getCacheKey();
    if (freshnessEvaluator.isFresh(definition, cacheKey, direction)) {
      return;
    }
    if (distributedStore.isRebuildLeaseHeld(cacheKey)) {
      return;
    }
    if (freshnessEvaluator.shouldSkipRebuild(cacheKey, definition)) {
      return;
    }
    if (!distributedStore.tryClaimRebuild(cacheKey, staleBuildingMillis(definition))) {
      return;
    }
    long invalidationGenAtStart =
        distributedStore.getInvalidationGeneration(definition.getGraphId());
    try {
      publishActiveSnapshot(definition, snapshot, seeds, cacheKey, invalidationGenAtStart);
    } catch (RuntimeException e) {
      distributedStore.releaseRebuildClaim(cacheKey);
      throw e;
    }
  }

  public void scheduledRebuild(@Nonnull EntityGraphDefinition definition) {
    if (definition.getScope().getMode() != ScopeMode.FULL) {
      return;
    }
    for (GraphSnapshotSource source : EntityGraphSources.supportedBuildSources(definition)) {
      String key = EntityGraphCacheKeys.fullCacheKey(definition.getGraphId(), source);
      if (freshnessEvaluator.shouldSkipRebuild(key, definition)) {
        continue;
      }
      if (!distributedStore.tryClaimRebuild(key, staleBuildingMillis(definition))) {
        continue;
      }
      executeRebuild(definition, source, null, key, TraversalDirection.FORWARD);
    }
  }

  private void enqueueBackgroundRebuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String cacheKey,
      @Nullable Collection<String> seeds,
      @Nonnull TraversalDirection direction) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metrics ->
                metrics.incrementMicrometer(
                    "entity.graph.cache.rebuild.enqueued",
                    1,
                    "graphId",
                    definition.getGraphId(),
                    "execution",
                    "background"));
    rebuildExecutor.execute(
        () -> {
          try {
            executeRebuild(definition, source, seeds, cacheKey, direction);
          } catch (RuntimeException e) {
            log.warn(
                "Background entity graph rebuild failed for graphId={} cacheKey={}",
                definition.getGraphId(),
                cacheKey,
                e);
          }
        });
  }

  private void handleRebuildException(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nullable String publishKeyHint,
      @Nullable Collection<String> seeds,
      @Nonnull RuntimeException exception) {
    if (GraphBuildFailure.classify(failureReasonForException(exception))
        == GraphBuildFailure.Kind.TRANSIENT) {
      markFailedBuild(definition, source, publishKeyHint, seeds, "build_exception");
      return;
    }
    String claimKey =
        PartialGraphRebuildStrategy.rebuildClaimKey(
            definition.getGraphId(), source, publishKeyHint, seeds);
    if (claimKey != null) {
      distributedStore.releaseRebuildClaim(claimKey);
    }
  }

  private void markFailedBuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nullable String publishKeyHint,
      @Nullable Collection<String> seeds,
      @Nullable String failureReason) {
    String markerKey =
        PartialGraphRebuildStrategy.rebuildClaimKey(
            definition.getGraphId(), source, publishKeyHint, seeds);
    if (markerKey == null) {
      releaseOrphanedRebuildClaim(definition.getGraphId(), source, publishKeyHint, seeds);
      return;
    }
    CacheStatus status = GraphBuildFailure.statusForFailedBuild(failureReason);
    switch (status) {
      case COOLDOWN -> distributedStore.markCooldown(markerKey);
      case OVER_LIMIT -> {
        distributedStore.markOverLimit(markerKey);
        log.warn(
            "Entity graph cache over limit: graphId={} source={} cacheKey={} reason={}",
            definition.getGraphId(),
            source,
            markerKey,
            failureReason);
      }
      case INVALID -> {
        distributedStore.markInvalid(markerKey);
        log.warn(
            "Entity graph cache invalid configuration: graphId={} source={} cacheKey={} reason={}",
            definition.getGraphId(),
            source,
            markerKey,
            failureReason);
      }
    }
  }

  private void publishActiveSnapshot(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull EntityGraphSnapshot snapshot,
      @Nullable Collection<String> seeds,
      @Nullable String rebuildClaimKey,
      long invalidationGenAtStart) {
    String publishKey = snapshot.getCacheKey();
    String claimKey = rebuildClaimKey != null ? rebuildClaimKey : publishKey;

    if (distributedStore.getInvalidationGeneration(definition.getGraphId())
        != invalidationGenAtStart) {
      distributedStore.releaseRebuildClaim(claimKey);
      recordPublishSuppressedStale(definition.getGraphId());
      return;
    }

    if (distributedStore.shouldSkipPublish(publishKey, snapshot)) {
      distributedStore.releaseRebuildClaim(claimKey);
      hydrateLocalView(definition, publishKey, snapshot);
      return;
    }

    distributedStore.publish(snapshot, CacheStatus.ACTIVE);
    distributedStore.releaseRebuildClaim(claimKey);
    if (definition.getScope().getMode() == ScopeMode.PARTIAL && seeds != null && !seeds.isEmpty()) {
      distributedStore.dropFailureMarkerKeysForRoots(
          definition.getGraphId(), definition.getBuildSource(), seeds);
    }
    EntityGraphView view = viewFromEdges(snapshot.getEdges());
    if (view != null) {
      localViews.put(
          definition.getGraphId(),
          publishKey,
          view,
          distributedStore.getGeneration(publishKey),
          definition.getLocalEviction());
    }
  }

  @Nonnull
  private static String failureReasonForException(@Nonnull RuntimeException exception) {
    if (exception instanceof IllegalStateException
        || exception instanceof IllegalArgumentException) {
      return "invalid_configuration";
    }
    return "build_exception";
  }

  /**
   * Releases a {@link CacheStatus#BUILDING} lease when failure handling has no status key target.
   */
  private void releaseOrphanedRebuildClaim(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nullable String publishKeyHint,
      @Nullable Collection<String> seeds) {
    if (publishKeyHint != null) {
      distributedStore.releaseRebuildClaim(publishKeyHint);
      return;
    }
    if (seeds != null && !seeds.isEmpty()) {
      distributedStore.releaseRebuildClaim(
          PartialGraphRebuildStrategy.failureMarkerKey(graphId, source, seeds.iterator().next()));
    }
  }

  @Nullable
  private EntityGraphView hydrateLocalView(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull EntityGraphSnapshot snapshot) {
    EntityGraphView view = viewFromEdges(snapshot.getEdges());
    if (view == null) {
      return null;
    }
    localViews.put(
        definition.getGraphId(),
        cacheKey,
        view,
        snapshot.getGeneration(),
        definition.getLocalEviction());
    return view;
  }

  private void recordPublishSuppressedStale(@Nonnull String graphId) {
    systemOperationContext
        .getMetricUtils()
        .ifPresent(
            metrics ->
                metrics.incrementMicrometer(
                    "entity.graph.cache.publish_suppressed_stale", 1, "graphId", graphId));
    log.debug(
        "Suppressed stale entity graph cache publish for graphId {} after invalidation", graphId);
  }
}
