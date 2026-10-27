package com.linkedin.metadata.graph.cache.service.read;

import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.service.rebuild.GraphCacheRebuilder;
import com.linkedin.metadata.graph.cache.service.strategy.PartialGraphScopeStrategy;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** PARTIAL-scope read helpers: per-root freshness, multi-component expand, ephemeral builds. */
public class PartialGraphReadBackend {
  private final GraphReadBackend shared;
  private final EntityGraphDistributedStore distributedStore;
  private final EntityGraphSnapshotBuilder snapshotBuilder;
  private final OperationContext systemOperationContext;
  private final SnapshotFreshnessEvaluator freshnessEvaluator;
  private final GraphCacheRebuilder rebuilder;

  public PartialGraphReadBackend(
      @Nonnull GraphReadBackend shared,
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull SnapshotFreshnessEvaluator freshnessEvaluator,
      @Nonnull GraphCacheRebuilder rebuilder) {
    this.shared = shared;
    this.distributedStore = distributedStore;
    this.snapshotBuilder = snapshotBuilder;
    this.systemOperationContext = systemOperationContext;
    this.freshnessEvaluator = freshnessEvaluator;
    this.rebuilder = rebuilder;
  }

  @Nonnull
  public GraphReadBackend shared() {
    return shared;
  }

  public void ensureFreshForRoot(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String root,
      @Nonnull TraversalDirection direction) {
    Set<String> rootSet = Set.of(root);
    Optional<String> existing =
        shared.findCacheKeyForSeeds(definition.getGraphId(), source, rootSet);
    if (existing.isPresent()) {
      rebuilder.ensureFreshKey(definition, source, existing.get(), rootSet, direction);
      return;
    }
    String markerKey =
        PartialGraphScopeStrategy.failureMarkerKey(definition.getGraphId(), source, root);
    if (freshnessEvaluator.shouldSkipRebuild(markerKey, definition)) {
      return;
    }
    if (!distributedStore.tryClaimRebuild(
        markerKey, SnapshotFreshnessEvaluator.staleBuildingMillis(definition))) {
      return;
    }
    rebuilder.executeRebuild(definition, source, rootSet, null, direction);
  }

  public boolean allRootsCacheFresh(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> roots) {
    for (String root : roots) {
      Optional<String> cacheKey =
          shared.findCacheKeyForSeeds(definition.getGraphId(), source, Set.of(root));
      if (cacheKey.isEmpty()
          || !freshnessEvaluator.isFresh(definition, cacheKey.get(), direction)) {
        return false;
      }
    }
    return true;
  }

  public boolean allRootsActive(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull Set<String> roots) {
    for (String root : roots) {
      Optional<String> cacheKey =
          shared.findCacheKeyForSeeds(definition.getGraphId(), source, Set.of(root));
      if (cacheKey.isEmpty() || distributedStore.getStatus(cacheKey.get()) != CacheStatus.ACTIVE) {
        return false;
      }
    }
    return true;
  }

  @Nonnull
  public List<GraphComponentContext> resolveComponentContexts(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull Set<String> roots) {
    List<GraphComponentContext> contexts = new ArrayList<>();
    Set<String> seenKeys = new LinkedHashSet<>();
    for (String root : roots) {
      Optional<String> cacheKey =
          shared.findCacheKeyForSeeds(definition.getGraphId(), source, Set.of(root));
      if (cacheKey.isEmpty() || !seenKeys.add(cacheKey.get())) {
        if (cacheKey.isEmpty()) {
          return List.of();
        }
        continue;
      }
      GraphComponentContext component = shared.resolveComponent(definition, cacheKey.get());
      if (component == null) {
        return List.of();
      }
      contexts.add(
          new GraphComponentContext(component.view(), component.coverage(), cacheKey.get()));
    }
    return contexts;
  }

  @Nonnull
  public ReadMissReason firstRootMissReason(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> roots) {
    for (String root : roots) {
      Optional<String> cacheKey =
          shared.findCacheKeyForSeeds(definition.getGraphId(), source, Set.of(root));
      if (cacheKey.isEmpty()) {
        return ReadMissReason.ABSENT;
      }
      ReadMissReason freshnessReason =
          shared.missReasonFromFreshness(definition, cacheKey.get(), direction);
      if (freshnessReason != null) {
        return freshnessReason;
      }
      GraphComponentContext component = shared.resolveComponent(definition, cacheKey.get());
      if (component == null) {
        return ReadMissReason.ABSENT;
      }
      if (!SnapshotFreshnessEvaluator.coverageSatisfies(
          definition, component.coverage(), direction)) {
        return ReadMissReason.INSUFFICIENT_COVERAGE;
      }
    }
    return ReadMissReason.ABSENT;
  }

  @Nullable
  public GraphComponentContext buildPartialComponentAndWarm(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull Set<String> roots,
      @Nonnull TraversalDirection direction) {
    BuildResult result =
        snapshotBuilder.buildPartial(
            systemOperationContext, definition, source, roots, direction, null, null, null);
    if (result.getStatus() != CacheStatus.ACTIVE || result.getSnapshot() == null) {
      return null;
    }
    rebuilder.warmCacheIfMissingOrStale(definition, result.getSnapshot(), direction, roots);
    EntityGraphView view = GraphComponentContext.viewFromEdges(result.getSnapshot().getEdges());
    if (view == null) {
      return null;
    }
    return GraphComponentContext.fromSnapshot(result.getSnapshot(), view);
  }
}
