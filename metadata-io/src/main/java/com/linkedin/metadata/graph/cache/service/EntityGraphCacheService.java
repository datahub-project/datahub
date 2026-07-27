package com.linkedin.metadata.graph.cache.service;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.KnownEntityGraph;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationBatch;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.service.invalidation.GraphCacheInvalidator;
import com.linkedin.metadata.graph.cache.service.read.GraphCacheReader;
import com.linkedin.metadata.graph.cache.service.rebuild.GraphCacheRebuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Core entity graph cache: expand, rebuild, and sync invalidation.
 *
 * <p>Invalidation design and extension guide: {@code docs/deploy/gms-entity-graph-cache.md}
 * (Invalidation — For implementers).
 */
@Slf4j
public class EntityGraphCacheService implements EntityGraphCache {

  private final EntityGraphCacheProperties properties;
  private final EntityGraphRegistry registry;
  private final EntityGraphDistributedStore distributedStore;
  private final EntityGraphLocalViewCache localViews;
  private final GraphCacheReader reader;
  private final GraphCacheRebuilder rebuilder;
  private final GraphCacheInvalidator invalidator;

  public EntityGraphCacheService(
      @Nonnull EntityGraphCacheProperties properties,
      @Nonnull EntityGraphRegistry registry,
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphLocalViewCache localViews,
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull ExecutorService rebuildExecutor) {
    this.properties = properties;
    this.registry = registry;
    this.distributedStore = distributedStore;
    this.localViews = localViews;
    SnapshotFreshnessEvaluator freshnessEvaluator =
        new SnapshotFreshnessEvaluator(distributedStore);
    this.rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            rebuildExecutor,
            freshnessEvaluator);
    this.reader =
        new GraphCacheReader(
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            freshnessEvaluator,
            rebuilder);
    this.invalidator =
        new GraphCacheInvalidator(registry, distributedStore, localViews, systemOperationContext);
  }

  @Override
  @Nonnull
  public GraphReadResult expand(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull Collection<String> roots,
      int limit,
      int maxDepth,
      @Nonnull ReadMode mode) {
    if (!properties.isEnabled()) {
      return GraphReadResult.miss(ReadMissReason.DISABLED);
    }
    EntityGraphDefinition definition = registry.getDefinition(graphId);
    if (definition == null) {
      return GraphReadResult.miss(ReadMissReason.INVALID_REQUEST);
    }
    return reader.expand(definition, source, direction, roots, limit, maxDepth, mode);
  }

  @Override
  @Nonnull
  public AncestorWalkResult walkOrderedForwardAncestors(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth,
      @Nonnull ReadMode mode) {
    if (!properties.isEnabled()) {
      return AncestorWalkResult.miss(ReadMissReason.DISABLED);
    }
    EntityGraphDefinition definition = registry.getDefinition(graphId);
    if (definition == null) {
      return AncestorWalkResult.miss(ReadMissReason.INVALID_REQUEST);
    }
    return reader.walkOrderedForwardAncestors(definition, source, seed, maxDepth, mode);
  }

  @Override
  @Nonnull
  public MembershipNeighborResult listRelated(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count,
      @Nonnull ReadMode mode) {
    if (!properties.isEnabled()) {
      return MembershipNeighborResult.miss(ReadMissReason.DISABLED);
    }
    EntityGraphDefinition definition = registry.getDefinition(graphId);
    if (definition == null) {
      return MembershipNeighborResult.miss(ReadMissReason.INVALID_REQUEST);
    }
    return reader.listRelated(
        definition, source, seedUrn, direction, relationshipTypes, maxDepth, start, count, mode);
  }

  @Override
  @Nonnull
  public Optional<EntityGraphBinding> bindingForKnownGraph(@Nonnull KnownEntityGraph graph) {
    EntityGraphBinding binding = registry.resolveKnownGraph(graph);
    return binding == null ? Optional.empty() : Optional.of(binding);
  }

  @Override
  @Nonnull
  public Optional<EntityGraphBinding> bindingForFilterField(@Nonnull String searchField) {
    EntityGraphBinding binding = registry.getBindingForFilterField(searchField);
    return binding == null ? Optional.empty() : Optional.of(binding);
  }

  @Override
  @Nonnull
  public Optional<EntityGraphBinding> bindingForPolicyField(@Nonnull String fieldType) {
    EntityGraphBinding binding =
        registry.getBindingForPolicyField(fieldType.toUpperCase(Locale.ROOT));
    return binding == null ? Optional.empty() : Optional.of(binding);
  }

  public void scheduledRebuild(@Nonnull EntityGraphDefinition definition) {
    if (!properties.isEnabled()) {
      return;
    }
    rebuilder.scheduledRebuild(definition);
  }

  @Override
  @Nonnull
  public Set<String> getCandidateGraphIds(@Nonnull String entityType, @Nonnull String aspectName) {
    return registry.getCandidateGraphIds(entityType, aspectName);
  }

  @Override
  @Nonnull
  public Set<String> getGraphIdsForEntityType(@Nonnull String entityType) {
    return registry.getGraphIdsForEntityType(entityType);
  }

  @Override
  public void invalidateOnSyncBatch(@Nonnull SyncGraphInvalidationBatch batch) {
    if (!properties.isEnabled()) {
      return;
    }
    invalidator.invalidateOnSyncBatch(batch);
  }

  public void onSnapshotUpdated(@Nonnull String cacheKey) {
    distributedStore.refreshPartialSeedIndex(cacheKey);
    localViews.evict(cacheKey);
  }

  public EntityGraphLocalViewCache getLocalViews() {
    return localViews;
  }
}
