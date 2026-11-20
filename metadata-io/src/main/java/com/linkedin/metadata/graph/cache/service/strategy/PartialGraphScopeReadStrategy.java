package com.linkedin.metadata.graph.cache.service.strategy;

import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.service.read.GraphReadDepthResolver;
import com.linkedin.metadata.graph.cache.service.read.PartialGraphReadBackend;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

/** PARTIAL-scope orchestration for cached and ephemeral graph reads. */
public class PartialGraphScopeReadStrategy implements GraphScopeReadStrategy {
  private final PartialGraphReadBackend partialBackend;

  public PartialGraphScopeReadStrategy(@Nonnull PartialGraphReadBackend partialBackend) {
    this.partialBackend = partialBackend;
  }

  @Nonnull
  @Override
  public GraphReadResult expandCached(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth) {
    Set<String> normalizedRoots = partialBackend.shared().rootsFrom(roots);
    for (String root : normalizedRoots) {
      partialBackend.ensureFreshForRoot(definition, source, root, direction);
    }
    if (!partialBackend.allRootsActive(definition, source, normalizedRoots)) {
      return miss(
          definition,
          partialBackend.firstRootMissReason(definition, source, direction, normalizedRoots));
    }

    List<GraphComponentContext> components =
        partialBackend.resolveComponentContexts(definition, source, normalizedRoots);
    if (components.isEmpty()) {
      return miss(
          definition,
          partialBackend.firstRootMissReason(definition, source, direction, normalizedRoots));
    }
    GraphReadResult result =
        partialBackend
            .shared()
            .expandFromComponents(definition, direction, roots, limit, maxDepth, components);
    recordExpand(definition, source, direction, normalizedRoots, result);
    return result;
  }

  /**
   * Serves a full-path read without calling {@code ensureFreshForRoot}. A trusted walk follows only
   * the edges that walk stored. A positive depth the builder already explored completely is served
   * from that limited walk. An unlimited read without a covering trusted walk misses and does not
   * rebuild.
   */
  @Nonnull
  @Override
  public GraphReadResult expandFullPath(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth) {
    Set<String> normalizedRoots = partialBackend.shared().rootsFrom(roots);
    if (normalizedRoots.isEmpty()) {
      return GraphReadResult.miss(ReadMissReason.INVALID_REQUEST);
    }
    String cacheKey = null;
    for (String root : normalizedRoots) {
      Optional<String> resolved =
          partialBackend
              .shared()
              .findCacheKeyForSeeds(definition.getGraphId(), source, Set.of(root));
      if (resolved.isEmpty()) {
        return GraphReadResult.miss(ReadMissReason.ABSENT);
      }
      if (cacheKey == null) {
        cacheKey = resolved.get();
      } else if (!cacheKey.equals(resolved.get())) {
        return GraphReadResult.miss(ReadMissReason.INSUFFICIENT_COVERAGE);
      }
    }
    ReadMissReason freshnessMiss =
        partialBackend.shared().missReasonFromFreshness(definition, cacheKey, direction);
    if (freshnessMiss != null) {
      return GraphReadResult.miss(freshnessMiss);
    }
    GraphComponentContext component =
        partialBackend.shared().resolveComponent(definition, cacheKey);
    if (component == null || !component.view().containsAllSeeds(normalizedRoots)) {
      return GraphReadResult.miss(ReadMissReason.ABSENT);
    }
    TraversalCoverage coverage = component.coverage();
    TraversalCoverage.DirectionCoverage stamped =
        coverage == null ? null : coverage.getDirection(direction);
    if (stamped != null
        && stamped.isTrustedFullWalk()
        && component
            .view()
            .coversRoots(
                direction,
                stamped.getTrustedSeeds(),
                normalizedRoots,
                stamped.getTrustedEdgeLines())) {
      int effectiveDepth =
          maxDepth == EntityGraphCache.USE_DEFINITION_MAX_DEPTH || maxDepth <= 0
              ? Integer.MAX_VALUE
              : maxDepth;
      return partialBackend
          .shared()
          .expandAtDepth(
              definition,
              direction,
              normalizedRoots,
              limit,
              maxDepth,
              component.view(),
              effectiveDepth,
              new HashSet<>(stamped.getTrustedEdgeLines()));
    }
    if (maxDepth > 0
        && stamped != null
        && stamped.isExplored()
        && stamped.isComplete()
        && stamped.getExploredDepth() >= maxDepth) {
      return partialBackend
          .shared()
          .expandAtDepth(
              definition, direction, normalizedRoots, limit, maxDepth, component.view(), maxDepth);
    }
    return GraphReadResult.miss(ReadMissReason.INSUFFICIENT_COVERAGE);
  }

  @Nonnull
  @Override
  public GraphReadResult expandEphemeral(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth) {
    Set<String> normalizedRoots = partialBackend.shared().rootsFrom(roots);
    if (partialBackend.allRootsCacheFresh(definition, source, direction, normalizedRoots)) {
      List<GraphComponentContext> components =
          partialBackend.resolveComponentContexts(definition, source, normalizedRoots);
      if (!components.isEmpty()) {
        GraphReadResult fromCache =
            partialBackend
                .shared()
                .expandFromComponents(definition, direction, roots, limit, maxDepth, components);
        if (fromCache.isHit()) {
          return fromCache;
        }
      }
    }

    List<GraphComponentContext> components = new ArrayList<>();
    for (String root : normalizedRoots) {
      GraphComponentContext component =
          partialBackend.buildPartialComponentAndWarm(definition, source, Set.of(root), direction);
      if (component != null) {
        components.add(component);
      }
    }
    if (components.isEmpty()) {
      return GraphReadResult.miss(ReadMissReason.ABSENT);
    }
    return partialBackend
        .shared()
        .expandFromComponents(definition, direction, roots, limit, maxDepth, components);
  }

  @Nonnull
  @Override
  public AncestorWalkResult walkCached(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth) {
    partialBackend.ensureFreshForRoot(definition, source, seed, TraversalDirection.FORWARD);
    Optional<String> cacheKey =
        partialBackend.shared().findCacheKeyForSeeds(definition.getGraphId(), source, Set.of(seed));
    if (cacheKey.isEmpty()) {
      return walkMiss(definition, ReadMissReason.ABSENT);
    }
    partialBackend
        .shared()
        .ensureFreshKey(
            definition, source, cacheKey.get(), Set.of(seed), TraversalDirection.FORWARD);
    AncestorWalkResult freshnessMiss =
        partialBackend
            .shared()
            .walkMissFromFreshness(definition, cacheKey.get(), TraversalDirection.FORWARD);
    if (freshnessMiss != null) {
      partialBackend
          .shared()
          .recordCachedRead(definition, "miss", ((AncestorWalkResult.Miss) freshnessMiss).reason());
      return freshnessMiss;
    }
    GraphComponentContext component =
        partialBackend.shared().resolveComponent(definition, cacheKey.get());
    if (component == null) {
      return walkMiss(definition, ReadMissReason.ABSENT);
    }
    if (!partialBackend
        .shared()
        .coverageSatisfies(definition, component.coverage(), TraversalDirection.FORWARD)) {
      return walkMiss(definition, ReadMissReason.INSUFFICIENT_COVERAGE);
    }
    AncestorWalkResult hit =
        AncestorWalkResult.fromAncestors(
            component
                .view()
                .orderedForwardAncestors(
                    seed, GraphReadDepthResolver.resolve(definition, maxDepth)));
    partialBackend
        .shared()
        .recordCachedRead(
            definition,
            partialBackend.shared().isFresh(definition, cacheKey.get(), TraversalDirection.FORWARD)
                ? "hit"
                : "stale_hit",
            null);
    return hit;
  }

  @Nonnull
  private GraphReadResult miss(
      @Nonnull EntityGraphDefinition definition, @Nonnull ReadMissReason reason) {
    partialBackend.shared().recordCachedRead(definition, "miss", reason);
    return GraphReadResult.miss(reason);
  }

  private void recordExpand(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> roots,
      @Nonnull GraphReadResult result) {
    if (result.isMiss()) {
      partialBackend
          .shared()
          .recordCachedRead(definition, "miss", ((GraphReadResult.Miss) result).reason());
      return;
    }
    boolean fresh = partialBackend.allRootsCacheFresh(definition, source, direction, roots);
    partialBackend.shared().recordCachedRead(definition, fresh ? "hit" : "stale_hit", null);
  }

  @Nonnull
  private AncestorWalkResult walkMiss(
      @Nonnull EntityGraphDefinition definition, @Nonnull ReadMissReason reason) {
    partialBackend.shared().recordCachedRead(definition, "miss", reason);
    return AncestorWalkResult.miss(reason);
  }

  @Nonnull
  @Override
  public AncestorWalkResult walkEphemeral(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth) {
    Optional<String> cacheKey =
        partialBackend.shared().findCacheKeyForSeeds(definition.getGraphId(), source, Set.of(seed));
    if (cacheKey.isPresent()
        && partialBackend
            .shared()
            .isFresh(definition, cacheKey.get(), TraversalDirection.FORWARD)) {
      GraphComponentContext component =
          partialBackend.shared().resolveComponent(definition, cacheKey.get());
      if (component != null
          && partialBackend
              .shared()
              .coverageSatisfies(definition, component.coverage(), TraversalDirection.FORWARD)) {
        return AncestorWalkResult.fromAncestors(
            component
                .view()
                .orderedForwardAncestors(
                    seed, GraphReadDepthResolver.resolve(definition, maxDepth)));
      }
    }

    GraphComponentContext component =
        partialBackend.buildPartialComponentAndWarm(
            definition, source, Set.of(seed), TraversalDirection.FORWARD);
    if (component == null) {
      return AncestorWalkResult.miss(ReadMissReason.ABSENT);
    }
    return AncestorWalkResult.fromAncestors(
        component
            .view()
            .orderedForwardAncestors(seed, GraphReadDepthResolver.resolve(definition, maxDepth)));
  }
}
