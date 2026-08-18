package com.linkedin.metadata.graph.cache.service.read;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.service.rebuild.GraphCacheRebuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Shared read operations used by scope-specific graph read strategies. */
@Slf4j
public class GraphReadBackend {
  private static final ThreadLocal<Integer> SEED_LOOKUP_MEMO_DEPTH =
      ThreadLocal.withInitial(() -> 0);
  private static final ThreadLocal<Map<String, Optional<String>>> SEED_LOOKUP_MEMO =
      ThreadLocal.withInitial(HashMap::new);

  private final EntityGraphDistributedStore distributedStore;
  private final EntityGraphLocalViewCache localViews;
  private final EntityGraphSnapshotBuilder snapshotBuilder;
  private final OperationContext systemOperationContext;
  private final SnapshotFreshnessEvaluator freshnessEvaluator;
  private final GraphCacheRebuilder rebuilder;

  public GraphReadBackend(
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphLocalViewCache localViews,
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull SnapshotFreshnessEvaluator freshnessEvaluator,
      @Nonnull GraphCacheRebuilder rebuilder) {
    this.distributedStore = distributedStore;
    this.localViews = localViews;
    this.snapshotBuilder = snapshotBuilder;
    this.systemOperationContext = systemOperationContext;
    this.freshnessEvaluator = freshnessEvaluator;
    this.rebuilder = rebuilder;
  }

  public void ensureFreshKey(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String cacheKey,
      @Nullable Set<String> roots,
      @Nonnull TraversalDirection direction) {
    rebuilder.ensureFreshKey(definition, source, cacheKey, roots, direction);
  }

  public boolean isFresh(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction) {
    return freshnessEvaluator.isFresh(definition, cacheKey, direction);
  }

  public boolean coverageSatisfies(
      @Nonnull EntityGraphDefinition definition,
      @Nullable TraversalCoverage coverage,
      @Nonnull TraversalDirection direction) {
    return SnapshotFreshnessEvaluator.coverageSatisfies(definition, coverage, direction);
  }

  @Nonnull
  public Set<String> rootsFrom(@Nonnull Collection<String> roots) {
    return GraphComponentContext.rootsFrom(roots);
  }

  @Nonnull
  public GraphReadResult expandFromComponent(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth,
      @Nonnull GraphComponentContext component,
      @Nonnull String cacheKey) {
    return expandFromComponents(
        definition, direction, roots, limit, maxDepth, List.of(component.withCacheKey(cacheKey)));
  }

  @Nonnull
  public GraphReadResult expandFromComponents(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth,
      @Nonnull List<GraphComponentContext> components) {
    for (GraphComponentContext component : components) {
      if (!SnapshotFreshnessEvaluator.coverageSatisfies(
          definition, component.coverage(), direction)) {
        return GraphReadResult.miss(ReadMissReason.INSUFFICIENT_COVERAGE);
      }
    }
    EntityGraphView view =
        components.size() == 1
            ? components.get(0).view()
            : EntityGraphView.fromComponents(
                components.stream().map(GraphComponentContext::view).toList());
    return expandFromView(definition, direction, roots, limit, maxDepth, view, components);
  }

  @Nonnull
  public MembershipNeighborResult listRelatedFromComponent(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull TraversalDirection direction,
      @Nonnull String seedUrn,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count,
      @Nonnull GraphComponentContext component) {
    if (!SnapshotFreshnessEvaluator.coverageSatisfies(
        definition, component.coverage(), direction)) {
      return MembershipNeighborResult.miss(ReadMissReason.INSUFFICIENT_COVERAGE);
    }
    return listRelatedFromView(
        direction, seedUrn, relationshipTypes, maxDepth, start, count, component.view());
  }

  @Nonnull
  public MembershipNeighborResult listRelatedFromView(
      @Nonnull TraversalDirection direction,
      @Nonnull String seedUrn,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count,
      @Nonnull EntityGraphView view) {
    if (!view.containsVertex(seedUrn)) {
      return MembershipNeighborResult.miss(ReadMissReason.ABSENT);
    }
    EntityGraphView.NeighborResult neighborResult =
        view.neighborsWithResult(direction, seedUrn, relationshipTypes, maxDepth, start, count);
    List<MembershipNeighborResult.Neighbor> neighbors =
        neighborResult.getNeighbors().stream()
            .map(
                edge ->
                    new MembershipNeighborResult.Neighbor(
                        direction == TraversalDirection.FORWARD
                            ? edge.getDestinationUrn()
                            : edge.getSourceUrn(),
                        edge.getRelationshipType()))
            .toList();
    return MembershipNeighborResult.fromNeighbors(neighbors, neighborResult.getTotal());
  }

  @Nullable
  public MembershipNeighborResult tryListRelatedFromFreshCache(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction,
      @Nonnull String seedUrn,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count) {
    if (!freshnessEvaluator.isFresh(definition, cacheKey, direction)) {
      return null;
    }
    GraphComponentContext component = resolveComponent(definition, cacheKey);
    if (component == null) {
      return null;
    }
    return listRelatedFromComponent(
        definition, direction, seedUrn, relationshipTypes, maxDepth, start, count, component);
  }

  @Nonnull
  public MembershipNeighborResult listRelatedEphemeralFromLiveFullBuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull String seedUrn,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count) {
    GraphComponentContext component = buildFullComponentAndWarm(definition, source, direction);
    if (component == null) {
      return MembershipNeighborResult.miss(ReadMissReason.ABSENT);
    }
    return listRelatedFromComponent(
        definition, direction, seedUrn, relationshipTypes, maxDepth, start, count, component);
  }

  @Nonnull
  public GraphReadResult expandFromView(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth,
      @Nonnull EntityGraphView view,
      @Nonnull List<GraphComponentContext> components) {
    Set<String> normalizedRoots = rootsFrom(roots);
    int effectiveDepth = resolveExpandDepth(definition, maxDepth, direction, components);
    logIncompleteCoverage(definition, direction, components);
    EntityGraphView.ExpandResult expandResult =
        view.expandWithResult(direction, normalizedRoots, limit, effectiveDepth);

    if (expandResult.isTruncatedByMaxDepth()
        && definition.getScope().getMode() == ScopeMode.PARTIAL
        && effectiveDepth == definition.getScope().getMaxDepth()) {
      log.warn(
          "Entity graph PARTIAL expand reached configured scope.maxDepth: graphId={} direction={} roots={} maxDepth={} resultSize={}",
          definition.getGraphId(),
          direction,
          normalizedRoots,
          effectiveDepth,
          expandResult.getVertices().size());
    } else if (expandResult.isTruncatedByMaxDepth() && maxDepth > 0) {
      log.warn(
          "Entity graph expand truncated by per-call maxDepth: graphId={} direction={} roots={} maxDepth={} resultSize={}",
          definition.getGraphId(),
          direction,
          normalizedRoots,
          maxDepth,
          expandResult.getVertices().size());
    }
    if (expandResult.isTruncatedByLimit()) {
      log.warn(
          "Entity graph expand truncated by per-call limit: graphId={} direction={} roots={} limit={} resultSize={}",
          definition.getGraphId(),
          direction,
          normalizedRoots,
          limit,
          expandResult.getVertices().size());
      return GraphReadResult.miss(ReadMissReason.TRUNCATED);
    }

    Set<String> expanded = expandResult.getVertices();
    if (expanded.isEmpty()) {
      return GraphReadResult.fromVertices(Collections.emptySet());
    }
    if (direction == TraversalDirection.REVERSE && expanded.equals(normalizedRoots)) {
      return GraphReadResult.fromVertices(Collections.emptySet());
    }
    return GraphReadResult.fromVertices(expanded);
  }

  private static int resolveExpandDepth(
      @Nonnull EntityGraphDefinition definition,
      int maxDepth,
      @Nonnull TraversalDirection direction,
      @Nonnull List<GraphComponentContext> components) {
    int callerDepth = GraphReadDepthResolver.resolve(definition, maxDepth);
    int coverageCap =
        components.stream()
            .map(GraphComponentContext::coverage)
            .filter(Objects::nonNull)
            .map(coverage -> coverage.getDirection(direction))
            .filter(Objects::nonNull)
            .mapToInt(TraversalCoverage.DirectionCoverage::getExploredDepth)
            .min()
            .orElse(Integer.MAX_VALUE);
    if (coverageCap == Integer.MAX_VALUE) {
      return callerDepth;
    }
    return Math.min(callerDepth, coverageCap);
  }

  @Nullable
  public GraphComponentContext resolveComponent(
      @Nonnull EntityGraphDefinition definition, @Nonnull String cacheKey) {
    EntityGraphSnapshot snapshot = distributedStore.getSnapshot(cacheKey);
    if (snapshot == null) {
      return null;
    }
    long generation = snapshot.getGeneration();
    EntityGraphView view =
        localViews
            .get(cacheKey, generation)
            .orElseGet(() -> hydrateLocalView(definition, cacheKey, snapshot));
    if (view == null) {
      return null;
    }
    return GraphComponentContext.fromSnapshot(snapshot, view);
  }

  @Nonnull
  public Optional<String> findCacheKeyForSeeds(
      @Nonnull String graphId, @Nonnull GraphSnapshotSource source, @Nonnull Set<String> seeds) {
    boolean ownsMemo = SEED_LOOKUP_MEMO_DEPTH.get() == 0;
    if (ownsMemo) {
      SEED_LOOKUP_MEMO.get().clear();
    }
    SEED_LOOKUP_MEMO_DEPTH.set(SEED_LOOKUP_MEMO_DEPTH.get() + 1);
    try {
      String memoKey = seedLookupMemoKey(graphId, source, seeds);
      Map<String, Optional<String>> memo = SEED_LOOKUP_MEMO.get();
      if (memo.containsKey(memoKey)) {
        return memo.get(memoKey);
      }
      Optional<String> resolved =
          distributedStore.findCacheKeyForSeeds(
              graphId,
              source,
              seeds,
              snapshot -> localViews.get(snapshot.getCacheKey(), snapshot.getGeneration()));
      memo.put(memoKey, resolved);
      return resolved;
    } finally {
      int depth = SEED_LOOKUP_MEMO_DEPTH.get() - 1;
      SEED_LOOKUP_MEMO_DEPTH.set(depth);
      if (depth == 0) {
        SEED_LOOKUP_MEMO.get().clear();
      }
    }
  }

  @Nonnull
  private static String seedLookupMemoKey(
      @Nonnull String graphId, @Nonnull GraphSnapshotSource source, @Nonnull Set<String> seeds) {
    return graphId
        + '\u001f'
        + source.name()
        + '\u001f'
        + String.join("\u001f", new TreeSet<>(seeds));
  }

  @Nullable
  public GraphReadResult missFromFreshness(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction) {
    ReadMissReason reason = missReasonFromFreshness(definition, cacheKey, direction);
    return reason == null ? null : GraphReadResult.miss(reason);
  }

  @Nullable
  public AncestorWalkResult walkMissFromFreshness(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction) {
    ReadMissReason reason = missReasonFromFreshness(definition, cacheKey, direction);
    return reason == null ? null : AncestorWalkResult.miss(reason);
  }

  @Nullable
  public ReadMissReason missReasonFromFreshness(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction) {
    if (freshnessEvaluator.expandBlocked(definition, cacheKey, direction)) {
      return ReadMissReason.STALE_BLOCKED;
    }
    CacheStatus status = distributedStore.getStatus(cacheKey);
    if (status == CacheStatus.COOLDOWN
        || status == CacheStatus.OVER_LIMIT
        || status == CacheStatus.INVALID) {
      return ReadMissReason.TOMBSTONE;
    }
    if (status == CacheStatus.ABSENT) {
      return ReadMissReason.ABSENT;
    }
    return null;
  }

  @Nullable
  public GraphReadResult tryExpandFromFreshCache(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth) {
    if (!freshnessEvaluator.isFresh(definition, cacheKey, direction)) {
      return null;
    }
    GraphComponentContext component = resolveComponent(definition, cacheKey);
    if (component == null) {
      return null;
    }
    if (!SnapshotFreshnessEvaluator.coverageSatisfies(
        definition, component.coverage(), direction)) {
      return GraphReadResult.miss(ReadMissReason.INSUFFICIENT_COVERAGE);
    }
    return expandFromComponent(definition, direction, roots, limit, maxDepth, component, cacheKey);
  }

  @Nullable
  public AncestorWalkResult tryWalkOrderedForwardAncestorsFromFreshCache(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull String seed,
      int maxDepth) {
    if (!freshnessEvaluator.isFresh(definition, cacheKey, TraversalDirection.FORWARD)) {
      return null;
    }
    GraphComponentContext component = resolveComponent(definition, cacheKey);
    if (component == null
        || !SnapshotFreshnessEvaluator.coverageSatisfies(
            definition, component.coverage(), TraversalDirection.FORWARD)) {
      return null;
    }
    return AncestorWalkResult.fromAncestors(
        component
            .view()
            .orderedForwardAncestors(seed, GraphReadDepthResolver.resolve(definition, maxDepth)));
  }

  @Nonnull
  public AncestorWalkResult walkOrderedForwardAncestorsFromLiveFullBuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth) {
    GraphComponentContext component =
        buildFullComponentAndWarm(definition, source, TraversalDirection.FORWARD);
    if (component == null) {
      return AncestorWalkResult.miss(ReadMissReason.ABSENT);
    }
    return AncestorWalkResult.fromAncestors(
        component
            .view()
            .orderedForwardAncestors(seed, GraphReadDepthResolver.resolve(definition, maxDepth)));
  }

  @Nonnull
  public GraphReadResult expandEphemeralFromLiveFullBuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth) {
    GraphComponentContext component = buildFullComponentAndWarm(definition, source, direction);
    if (component == null || component.cacheKey() == null) {
      return GraphReadResult.miss(ReadMissReason.ABSENT);
    }
    return expandFromComponent(
        definition, direction, roots, limit, maxDepth, component, component.cacheKey());
  }

  @Nullable
  private GraphComponentContext buildFullComponentAndWarm(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction) {
    BuildResult result = snapshotBuilder.build(systemOperationContext, definition, source, null);
    if (result.getStatus() != CacheStatus.ACTIVE || result.getSnapshot() == null) {
      return null;
    }
    rebuilder.warmCacheIfMissingOrStale(definition, result.getSnapshot(), direction, null);
    EntityGraphView view = GraphComponentContext.viewFromEdges(result.getSnapshot().getEdges());
    if (view == null) {
      return null;
    }
    return GraphComponentContext.fromSnapshot(result.getSnapshot(), view);
  }

  @Nullable
  private EntityGraphView hydrateLocalView(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull EntityGraphSnapshot snapshot) {
    EntityGraphView view = GraphComponentContext.viewFromEdges(snapshot.getEdges());
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

  private void logIncompleteCoverage(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull TraversalDirection direction,
      @Nonnull List<GraphComponentContext> components) {
    for (GraphComponentContext component : components) {
      TraversalCoverage coverage = component.coverage();
      if (coverage == null) {
        continue;
      }
      TraversalCoverage.DirectionCoverage directionCoverage = coverage.getDirection(direction);
      if (directionCoverage == null) {
        continue;
      }
      if (!directionCoverage.isComplete()) {
        log.warn(
            "Entity graph expand using incomplete coverage: graphId={} direction={} exploredDepth={} configuredMaxDepth={} truncationReason={}",
            definition.getGraphId(),
            direction,
            directionCoverage.getExploredDepth(),
            directionCoverage.getConfiguredMaxDepth(),
            directionCoverage.getTruncationReason());
      }
    }
  }
}
