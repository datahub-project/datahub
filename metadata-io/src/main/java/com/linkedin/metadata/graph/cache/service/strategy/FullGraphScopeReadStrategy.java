package com.linkedin.metadata.graph.cache.service.strategy;

import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.service.read.GraphReadBackend;
import com.linkedin.metadata.graph.cache.service.read.GraphReadDepthResolver;
import java.util.Collection;
import javax.annotation.Nonnull;

/** FULL-scope orchestration for cached and ephemeral graph reads. */
public class FullGraphScopeReadStrategy implements GraphScopeReadStrategy {
  private final GraphReadBackend readBackend;

  public FullGraphScopeReadStrategy(@Nonnull GraphReadBackend readBackend) {
    this.readBackend = readBackend;
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
    String cacheKey = FullGraphScopeStrategy.cacheKey(definition, source);
    readBackend.ensureFreshKey(definition, source, cacheKey, null, direction);
    GraphReadResult freshnessMiss = readBackend.missFromFreshness(definition, cacheKey, direction);
    if (freshnessMiss != null) {
      readBackend.recordCachedRead(
          definition, "miss", ((GraphReadResult.Miss) freshnessMiss).reason());
      return freshnessMiss;
    }
    GraphComponentContext component = readBackend.resolveComponent(definition, cacheKey);
    if (component == null) {
      return readMiss(definition, ReadMissReason.ABSENT);
    }
    if (!readBackend.coverageSatisfies(definition, component.coverage(), direction)) {
      return readMiss(definition, ReadMissReason.INSUFFICIENT_COVERAGE);
    }
    GraphReadResult hit =
        readBackend.expandFromComponent(
            definition, direction, roots, limit, maxDepth, component, cacheKey);
    recordExpand(definition, cacheKey, direction, hit);
    return hit;
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
    String cacheKey = FullGraphScopeStrategy.cacheKey(definition, source);
    GraphReadResult fromCache =
        readBackend.tryExpandFromFreshCache(
            definition, cacheKey, direction, roots, limit, maxDepth);
    if (fromCache != null && fromCache.isHit()) {
      return fromCache;
    }
    return readBackend.expandEphemeralFromLiveFullBuild(
        definition, source, direction, roots, limit, maxDepth);
  }

  @Nonnull
  @Override
  public AncestorWalkResult walkCached(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth) {
    String cacheKey = FullGraphScopeStrategy.cacheKey(definition, source);
    readBackend.ensureFreshKey(definition, source, cacheKey, null, TraversalDirection.FORWARD);
    AncestorWalkResult freshnessMiss =
        readBackend.walkMissFromFreshness(definition, cacheKey, TraversalDirection.FORWARD);
    if (freshnessMiss != null) {
      readBackend.recordCachedRead(
          definition, "miss", ((AncestorWalkResult.Miss) freshnessMiss).reason());
      return freshnessMiss;
    }
    GraphComponentContext component = readBackend.resolveComponent(definition, cacheKey);
    if (component == null) {
      return walkMiss(definition, ReadMissReason.ABSENT);
    }
    if (!readBackend.coverageSatisfies(
        definition, component.coverage(), TraversalDirection.FORWARD)) {
      return walkMiss(definition, ReadMissReason.INSUFFICIENT_COVERAGE);
    }
    AncestorWalkResult hit =
        AncestorWalkResult.fromAncestors(
            component
                .view()
                .orderedForwardAncestors(
                    seed, GraphReadDepthResolver.resolve(definition, maxDepth)));
    readBackend.recordCachedRead(
        definition,
        readBackend.isFresh(definition, cacheKey, TraversalDirection.FORWARD) ? "hit" : "stale_hit",
        null);
    return hit;
  }

  @Nonnull
  private GraphReadResult readMiss(
      @Nonnull EntityGraphDefinition definition, @Nonnull ReadMissReason reason) {
    readBackend.recordCachedRead(definition, "miss", reason);
    return GraphReadResult.miss(reason);
  }

  private void recordExpand(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull String cacheKey,
      @Nonnull TraversalDirection direction,
      @Nonnull GraphReadResult result) {
    if (result.isMiss()) {
      readBackend.recordCachedRead(definition, "miss", ((GraphReadResult.Miss) result).reason());
      return;
    }
    readBackend.recordCachedRead(
        definition,
        readBackend.isFresh(definition, cacheKey, direction) ? "hit" : "stale_hit",
        null);
  }

  @Nonnull
  private AncestorWalkResult walkMiss(
      @Nonnull EntityGraphDefinition definition, @Nonnull ReadMissReason reason) {
    readBackend.recordCachedRead(definition, "miss", reason);
    return AncestorWalkResult.miss(reason);
  }

  @Nonnull
  @Override
  public AncestorWalkResult walkEphemeral(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth) {
    String cacheKey = FullGraphScopeStrategy.cacheKey(definition, source);
    AncestorWalkResult fromCache =
        readBackend.tryWalkOrderedForwardAncestorsFromFreshCache(
            definition, cacheKey, seed, maxDepth);
    if (fromCache != null && fromCache.isHit()) {
      return fromCache;
    }
    return readBackend.walkOrderedForwardAncestorsFromLiveFullBuild(
        definition, source, seed, maxDepth);
  }
}
