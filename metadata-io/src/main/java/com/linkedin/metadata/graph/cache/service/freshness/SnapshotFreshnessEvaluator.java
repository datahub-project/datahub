package com.linkedin.metadata.graph.cache.service.freshness;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphBuildFailure;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Evaluates snapshot freshness and rebuild eligibility for a cache key. */
public final class SnapshotFreshnessEvaluator {

  private final EntityGraphDistributedStore distributedStore;

  public SnapshotFreshnessEvaluator(@Nonnull EntityGraphDistributedStore distributedStore) {
    this.distributedStore = distributedStore;
  }

  @Nonnull
  public SnapshotFreshness evaluate(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction) {
    CacheStatus status = distributedStore.getStatus(cacheKey);
    if (status == CacheStatus.ABSENT) {
      return SnapshotFreshness.ABSENT;
    }
    if (status == CacheStatus.COOLDOWN
        || status == CacheStatus.OVER_LIMIT
        || status == CacheStatus.INVALID) {
      return SnapshotFreshness.TOMBSTONE;
    }
    if (status != CacheStatus.ACTIVE) {
      return SnapshotFreshness.STALE_BLOCKED;
    }
    EntityGraphSnapshot snapshot = distributedStore.getSnapshot(cacheKey);
    if (snapshot == null) {
      return usesBackgroundRebuild(definition)
          ? SnapshotFreshness.STALE_BLOCKED
          : SnapshotFreshness.ABSENT;
    }
    if (isFreshSnapshot(definition, snapshot, direction)) {
      return SnapshotFreshness.FRESH;
    }
    if (isStale(snapshot, definition)) {
      return usesBackgroundRebuild(definition)
          ? SnapshotFreshness.STALE_BLOCKED
          : SnapshotFreshness.STALE_SERVABLE;
    }
    return SnapshotFreshness.STALE_BLOCKED;
  }

  public boolean isFresh(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction) {
    return evaluate(definition, cacheKey, direction) == SnapshotFreshness.FRESH;
  }

  public boolean expandBlocked(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction) {
    if (usesBackgroundRebuild(definition) && !isFresh(definition, cacheKey, direction)) {
      return true;
    }
    CacheStatus status = distributedStore.getStatus(cacheKey);
    if (status != CacheStatus.ACTIVE) {
      return false;
    }
    EntityGraphSnapshot snapshot = distributedStore.getSnapshot(cacheKey);
    if (snapshot == null) {
      return true;
    }
    return isStale(snapshot, definition) && !isFresh(definition, cacheKey, direction);
  }

  public boolean shouldSkipRebuild(
      @Nonnull String cacheKey, @Nonnull EntityGraphDefinition definition) {
    return shouldSkipRebuild(distributedStore.getStatus(cacheKey), cacheKey, definition);
  }

  public boolean shouldSkipRebuild(
      @Nonnull CacheStatus status,
      @Nonnull String cacheKey,
      @Nonnull EntityGraphDefinition definition) {
    if (GraphBuildFailure.suppressesAutomaticRebuild(status)) {
      return true;
    }
    return status == CacheStatus.COOLDOWN && !isCooldownExpired(cacheKey, definition);
  }

  public static boolean usesBackgroundRebuild(@Nonnull EntityGraphDefinition definition) {
    return definition.getScope().getMode() == ScopeMode.FULL
        && definition.getRebuildExecution() == RebuildExecution.BACKGROUND;
  }

  public static boolean isStale(
      @Nonnull EntityGraphSnapshot snapshot, @Nonnull EntityGraphDefinition definition) {
    return System.currentTimeMillis() - snapshot.getBuiltAtMillis()
        > definition.getPopulationIntervalSeconds() * 1000L;
  }

  public static long staleBuildingMillis(@Nonnull EntityGraphDefinition definition) {
    return Math.max(5L, definition.getPopulationIntervalSeconds()) * 1000L;
  }

  public static boolean coverageSatisfies(
      @Nonnull EntityGraphDefinition definition,
      @Nullable TraversalCoverage coverage,
      @Nonnull TraversalDirection direction) {
    if (definition.getScope().getMode() == ScopeMode.PARTIAL) {
      return coverage != null && coverage.canSatisfy(direction);
    }
    if (coverage == null) {
      return true;
    }
    return coverage.canSatisfy(direction);
  }

  private boolean isFreshSnapshot(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull EntityGraphSnapshot snapshot,
      @Nonnull TraversalDirection direction) {
    return !isStale(snapshot, definition)
        && coverageSatisfies(definition, snapshot.getTraversalCoverage(), direction);
  }

  private boolean isCooldownExpired(
      @Nonnull String cacheKey, @Nonnull EntityGraphDefinition definition) {
    return distributedStore
        .getCooldownRecordedAt(cacheKey)
        .map(
            recordedAt ->
                System.currentTimeMillis() - recordedAt
                    > definition.getPopulationIntervalSeconds() * 1000L)
        .orElse(true);
  }
}
