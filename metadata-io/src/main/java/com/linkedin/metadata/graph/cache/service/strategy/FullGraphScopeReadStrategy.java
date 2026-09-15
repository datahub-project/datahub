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
    GraphReadResult miss = readBackend.missFromFreshness(definition, cacheKey, direction);
    if (miss != null) {
      return miss;
    }
    GraphComponentContext component = readBackend.resolveComponent(definition, cacheKey);
    if (component == null) {
      return GraphReadResult.miss(ReadMissReason.ABSENT);
    }
    if (!readBackend.coverageSatisfies(definition, component.coverage(), direction)) {
      return GraphReadResult.miss(ReadMissReason.INSUFFICIENT_COVERAGE);
    }
    return readBackend.expandFromComponent(
        definition, direction, roots, limit, maxDepth, component, cacheKey);
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
    AncestorWalkResult miss =
        readBackend.walkMissFromFreshness(definition, cacheKey, TraversalDirection.FORWARD);
    if (miss != null) {
      return miss;
    }
    GraphComponentContext component = readBackend.resolveComponent(definition, cacheKey);
    if (component == null) {
      return AncestorWalkResult.miss(ReadMissReason.ABSENT);
    }
    if (!readBackend.coverageSatisfies(
        definition, component.coverage(), TraversalDirection.FORWARD)) {
      return AncestorWalkResult.miss(ReadMissReason.INSUFFICIENT_COVERAGE);
    }
    return AncestorWalkResult.fromAncestors(
        component
            .view()
            .orderedForwardAncestors(seed, GraphReadDepthResolver.resolve(definition, maxDepth)));
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
