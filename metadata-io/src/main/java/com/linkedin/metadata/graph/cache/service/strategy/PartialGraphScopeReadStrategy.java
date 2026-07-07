package com.linkedin.metadata.graph.cache.service.strategy;

import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.service.read.GraphReadDepthResolver;
import com.linkedin.metadata.graph.cache.service.read.PartialGraphReadBackend;
import java.util.ArrayList;
import java.util.Collection;
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
      return GraphReadResult.miss(
          partialBackend.firstRootMissReason(definition, source, direction, normalizedRoots));
    }

    List<GraphComponentContext> components =
        partialBackend.resolveComponentContexts(definition, source, normalizedRoots);
    if (components.isEmpty()) {
      return GraphReadResult.miss(
          partialBackend.firstRootMissReason(definition, source, direction, normalizedRoots));
    }
    return partialBackend
        .shared()
        .expandFromComponents(definition, direction, roots, limit, maxDepth, components);
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
      return AncestorWalkResult.miss(ReadMissReason.ABSENT);
    }
    partialBackend
        .shared()
        .ensureFreshKey(
            definition, source, cacheKey.get(), Set.of(seed), TraversalDirection.FORWARD);
    AncestorWalkResult miss =
        partialBackend
            .shared()
            .walkMissFromFreshness(definition, cacheKey.get(), TraversalDirection.FORWARD);
    if (miss != null) {
      return miss;
    }
    GraphComponentContext component =
        partialBackend.shared().resolveComponent(definition, cacheKey.get());
    if (component == null) {
      return AncestorWalkResult.miss(ReadMissReason.ABSENT);
    }
    if (!partialBackend
        .shared()
        .coverageSatisfies(definition, component.coverage(), TraversalDirection.FORWARD)) {
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
