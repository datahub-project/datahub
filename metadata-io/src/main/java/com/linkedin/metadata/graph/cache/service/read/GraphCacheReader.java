package com.linkedin.metadata.graph.cache.service.read;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphSources;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.service.rebuild.GraphCacheRebuilder;
import com.linkedin.metadata.graph.cache.service.strategy.FullGraphScopeStrategy;
import com.linkedin.metadata.graph.cache.service.strategy.GraphScopeReadStrategy;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Read/expand/walk operations for entity graph cache snapshots. */
@Slf4j
public class GraphCacheReader {
  private final GraphReadBackend readBackend;
  private final GraphScopeReadStrategy fullReadStrategy;
  private final GraphScopeReadStrategy partialReadStrategy;

  public GraphCacheReader(
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphLocalViewCache localViews,
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull SnapshotFreshnessEvaluator freshnessEvaluator,
      @Nonnull GraphCacheRebuilder rebuilder) {
    GraphCacheReaderFactory.GraphCacheReaderComponents components =
        GraphCacheReaderFactory.create(
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            freshnessEvaluator,
            rebuilder);
    this.readBackend = components.sharedReadBackend();
    this.fullReadStrategy = components.fullReadStrategy();
    this.partialReadStrategy = components.partialReadStrategy();
  }

  @Nonnull
  public GraphReadResult expand(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth,
      @Nonnull ReadMode mode) {
    if (definition == null
        || !definition.isEnabled()
        || !EntityGraphSources.supports(definition, source)
        || (definition.getScope().getMode() == ScopeMode.PARTIAL
            && (roots == null || roots.isEmpty()))) {
      return GraphReadResult.miss(ReadMissReason.INVALID_REQUEST);
    }
    GraphScopeReadStrategy strategy = strategyFor(definition);

    if (mode == ReadMode.EPHEMERAL) {
      return strategy.expandEphemeral(definition, source, direction, roots, limit, maxDepth);
    }
    return strategy.expandCached(definition, source, direction, roots, limit, maxDepth);
  }

  @Nonnull
  public AncestorWalkResult walkOrderedForwardAncestors(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth,
      @Nonnull ReadMode mode) {
    if (definition == null
        || !definition.isEnabled()
        || maxDepth <= 0
        || seed.isBlank()
        || !EntityGraphSources.supports(definition, source)) {
      return AncestorWalkResult.miss(ReadMissReason.INVALID_REQUEST);
    }
    GraphScopeReadStrategy strategy = strategyFor(definition);
    if (mode == ReadMode.EPHEMERAL) {
      return strategy.walkEphemeral(definition, source, seed, maxDepth);
    }
    return strategy.walkCached(definition, source, seed, maxDepth);
  }

  @Nonnull
  public MembershipNeighborResult listRelated(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count,
      @Nonnull ReadMode mode) {
    if (definition == null
        || !definition.isEnabled()
        || seedUrn.isBlank()
        || relationshipTypes.isEmpty()
        || maxDepth <= 0
        || count <= 0
        || !EntityGraphSources.supports(definition, source)
        || definition.getScope().getMode() != ScopeMode.FULL) {
      return MembershipNeighborResult.miss(ReadMissReason.INVALID_REQUEST);
    }
    if (mode == ReadMode.EPHEMERAL) {
      return listRelatedEphemeral(
          definition, source, seedUrn, direction, relationshipTypes, maxDepth, start, count);
    }
    return listRelatedCached(
        definition, source, seedUrn, direction, relationshipTypes, maxDepth, start, count);
  }

  @Nonnull
  private MembershipNeighborResult listRelatedCached(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count) {
    String cacheKey = FullGraphScopeStrategy.cacheKey(definition, source);
    readBackend.ensureFreshKey(definition, source, cacheKey, null, direction);
    ReadMissReason missReason =
        readBackend.missReasonFromFreshness(definition, cacheKey, direction);
    if (missReason != null) {
      return MembershipNeighborResult.miss(missReason);
    }
    GraphComponentContext component = readBackend.resolveComponent(definition, cacheKey);
    if (component == null) {
      return MembershipNeighborResult.miss(ReadMissReason.ABSENT);
    }
    return readBackend.listRelatedFromComponent(
        definition, direction, seedUrn, relationshipTypes, maxDepth, start, count, component);
  }

  @Nonnull
  private MembershipNeighborResult listRelatedEphemeral(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count) {
    String cacheKey = FullGraphScopeStrategy.cacheKey(definition, source);
    MembershipNeighborResult fromCache =
        readBackend.tryListRelatedFromFreshCache(
            definition, cacheKey, direction, seedUrn, relationshipTypes, maxDepth, start, count);
    if (fromCache != null && fromCache.isHit()) {
      return fromCache;
    }
    return readBackend.listRelatedEphemeralFromLiveFullBuild(
        definition, source, direction, seedUrn, relationshipTypes, maxDepth, start, count);
  }

  @Nonnull
  private GraphScopeReadStrategy strategyFor(@Nonnull EntityGraphDefinition definition) {
    return definition.getScope().getMode() == ScopeMode.FULL
        ? fullReadStrategy
        : partialReadStrategy;
  }
}
