package com.linkedin.metadata.graph.cache.service.rebuild;

import static com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator.staleBuildingMillis;
import static com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator.usesBackgroundRebuild;
import static com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator.usesStaleWhileRevalidate;
import static com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext.viewFromEdges;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphBuildFailure;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.GraphBounds;
import com.linkedin.metadata.graph.cache.config.EntityGraphSources;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Optional;
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
          && !SnapshotFreshnessEvaluator.isStaleOrSuperseded(distributedStore, snapshot, definition)
          && SnapshotFreshnessEvaluator.coverageSatisfies(
              definition, snapshot.getTraversalCoverage(), direction)) {
        return;
      }
    }
    if (!distributedStore.tryClaimRebuild(cacheKey, staleBuildingMillis(definition))) {
      return;
    }
    if (usesBackgroundRebuild(definition)) {
      enqueueBackgroundRebuild(definition, source, cacheKey, seeds, direction, "background");
      return;
    }
    if (usesStaleWhileRevalidate(definition) && distributedStore.hasServableSnapshot(cacheKey)) {
      enqueueBackgroundRebuild(
          definition, source, cacheKey, seeds, direction, "stale_while_revalidate");
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
    runRebuild(definition, source, seeds, publishKeyHint, direction, "sync");
  }

  private void runRebuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nullable Collection<String> seeds,
      @Nullable String publishKeyHint,
      @Nonnull TraversalDirection direction,
      @Nonnull String execution) {
    recordRebuild(definition, execution, "started");
    long startedAtNanos = System.nanoTime();
    try {
      boolean published = rebuildAndPublish(definition, source, seeds, publishKeyHint, direction);
      recordRebuild(definition, execution, published ? "completed" : "failed");
      recordRebuildDuration(definition, execution, startedAtNanos);
    } catch (RuntimeException e) {
      recordRebuild(definition, execution, "failed");
      recordRebuildDuration(definition, execution, startedAtNanos);
      throw e;
    }
  }

  private boolean rebuildAndPublish(
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
          return false;
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
        return false;
      }

      publishActiveSnapshot(
          definition, result.getSnapshot(), seeds, rebuildClaimKey, invalidationGenAtStart);
      return true;
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
      runRebuild(definition, source, null, key, TraversalDirection.FORWARD, "scheduled");
    }
  }

  private void enqueueBackgroundRebuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String cacheKey,
      @Nullable Collection<String> seeds,
      @Nonnull TraversalDirection direction,
      @Nonnull String execution) {
    recordRebuild(definition, execution, "enqueued");
    rebuildExecutor.execute(
        () -> {
          try {
            runRebuild(definition, source, seeds, cacheKey, direction, execution);
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
    String failureReason = failureReasonForException(exception);
    if (GraphBuildFailure.classify(failureReason) == GraphBuildFailure.Kind.TRANSIENT) {
      markFailedBuild(definition, source, publishKeyHint, seeds, "build_exception");
      return;
    }
    String claimKey =
        PartialGraphRebuildStrategy.rebuildClaimKey(
            definition.getGraphId(), source, publishKeyHint, seeds);
    // A config exception on a refresh that keeps the previous snapshot must suppress another
    // rebuild. Releasing the lease alone lets the next read enqueue immediately.
    if (claimKey != null
        && SnapshotFreshnessEvaluator.servesPreviousSnapshot(definition)
        && distributedStore.hasServableSnapshot(claimKey)) {
      markFailedBuild(definition, source, publishKeyHint, seeds, failureReason);
      return;
    }
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
    // A refresh that serves the previous snapshot must not replace it with an empty tombstone.
    // Suppression stops the retry loop; reads still see the embedded ACTIVE snapshot.
    if (SnapshotFreshnessEvaluator.servesPreviousSnapshot(definition)
        && distributedStore.hasServableSnapshot(markerKey)) {
      logFailedBuild(definition, source, markerKey, failureReason, status);
      distributedStore.suppressRebuildKeepingSnapshot(markerKey, status);
      return;
    }
    switch (status) {
      case COOLDOWN -> distributedStore.markCooldown(markerKey);
      case OVER_LIMIT -> {
        distributedStore.markOverLimit(markerKey);
        logFailedBuild(definition, source, markerKey, failureReason, status);
      }
      case INVALID -> {
        distributedStore.markInvalid(markerKey);
        logFailedBuild(definition, source, markerKey, failureReason, status);
      }
    }
  }

  private static void logFailedBuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String cacheKey,
      @Nullable String failureReason,
      @Nonnull CacheStatus status) {
    if (status == CacheStatus.OVER_LIMIT) {
      GraphBounds bounds = definition.getBounds();
      log.warn(
          "Entity graph cache over limit: graphId={} source={} cacheKey={} reason={} maxVertices={} maxEdges={}",
          definition.getGraphId(),
          source,
          cacheKey,
          failureReason,
          bounds == null ? "unset" : bounds.getMaxVertices(),
          bounds == null || bounds.getMaxEdges().isEmpty()
              ? "unset"
              : bounds.getMaxEdges().getAsInt());
    } else if (status == CacheStatus.INVALID) {
      log.warn(
          "Entity graph cache invalid configuration: graphId={} source={} cacheKey={} reason={}",
          definition.getGraphId(),
          source,
          cacheKey,
          failureReason);
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
      distributedStore.clearRebuildSuppression(publishKey);
      recordPublishSuppressedStale(definition.getGraphId());
      return;
    }

    if (distributedStore.shouldSkipPublish(publishKey, snapshot)) {
      // When the rebuild confirmed the stored topology, move its build time forward; otherwise a
      // snapshot older than a visibility watermark would be rebuilt again on every read.
      distributedStore.refreshBuiltAt(publishKey, snapshot);
      distributedStore.releaseRebuildClaim(claimKey);
      distributedStore.clearRebuildSuppression(publishKey);
      hydrateLocalView(definition, publishKey, snapshot);
      return;
    }

    boolean published = distributedStore.publish(snapshot, CacheStatus.ACTIVE);
    distributedStore.clearRebuildSuppression(publishKey);
    distributedStore.releaseRebuildClaim(claimKey);
    if (!published) {
      // A build that read its source later is already stored; this one finished late.
      return;
    }
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

  private void recordRebuild(
      @Nonnull EntityGraphDefinition definition, @Nonnull String execution, @Nonnull String event) {
    if (systemOperationContext == null) {
      return;
    }
    Optional<MetricUtils> metrics = systemOperationContext.getMetricUtils();
    if (metrics == null) {
      return;
    }
    String metricName =
        "enqueued".equals(event)
            ? "entity.graph.cache.rebuild.enqueued"
            : "entity.graph.cache.rebuild." + event;
    metrics.ifPresent(
        utils ->
            utils.incrementMicrometer(
                metricName, 1, "graphId", definition.getGraphId(), "execution", execution));
  }

  private void recordRebuildDuration(
      @Nonnull EntityGraphDefinition definition, @Nonnull String execution, long startedAtNanos) {
    if (systemOperationContext == null) {
      return;
    }
    Optional<MetricUtils> metrics = systemOperationContext.getMetricUtils();
    if (metrics == null) {
      return;
    }
    metrics.ifPresent(
        utils ->
            utils.recordTimer(
                "entity.graph.cache.rebuild.duration",
                System.nanoTime() - startedAtNanos,
                "graphId",
                definition.getGraphId(),
                "execution",
                execution));
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
