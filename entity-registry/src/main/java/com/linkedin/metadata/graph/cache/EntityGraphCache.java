package com.linkedin.metadata.graph.cache;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

/** Unified entity hierarchy graph cache API (implementation in metadata-io). */
public interface EntityGraphCache {

  /**
   * Request unbounded traversal within the materialized snapshot for FULL graphs, or use configured
   * {@code scope.maxDepth} for PARTIAL graphs. See {@code GraphReadDepthResolver}.
   */
  int USE_DEFINITION_MAX_DEPTH = -1;

  EntityGraphCache NO_OP =
      new EntityGraphCache() {
        @Override
        public GraphReadResult expand(
            @Nonnull String graphId,
            @Nonnull GraphSnapshotSource source,
            @Nonnull TraversalDirection direction,
            @Nonnull java.util.Collection<String> roots,
            int limit,
            int maxDepth,
            @Nonnull ReadMode mode) {
          return GraphReadResult.miss(ReadMissReason.DISABLED);
        }

        @Override
        public Optional<EntityGraphBinding> bindingForKnownGraph(@Nonnull KnownEntityGraph graph) {
          return Optional.empty();
        }

        @Override
        public Optional<EntityGraphBinding> bindingForFilterField(@Nonnull String searchField) {
          return Optional.empty();
        }

        @Override
        public Optional<EntityGraphBinding> bindingForPolicyField(@Nonnull String fieldType) {
          return Optional.empty();
        }

        @Override
        public Set<String> getCandidateGraphIds(
            @Nonnull String entityType, @Nonnull String aspectName) {
          return Collections.emptySet();
        }

        @Override
        public Set<String> getGraphIdsForEntityType(@Nonnull String entityType) {
          return Collections.emptySet();
        }

        @Override
        public void invalidateOnSyncBatch(@Nonnull SyncGraphInvalidationBatch batch) {}

        @Override
        @Nonnull
        public AncestorWalkResult walkOrderedForwardAncestors(
            @Nonnull String graphId,
            @Nonnull GraphSnapshotSource source,
            @Nonnull String seed,
            int maxDepth,
            @Nonnull ReadMode mode) {
          return AncestorWalkResult.miss(ReadMissReason.DISABLED);
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
          return MembershipNeighborResult.miss(ReadMissReason.DISABLED);
        }
      };

  @Nonnull
  default GraphReadResult expand(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull java.util.Collection<String> roots,
      int limit) {
    return expand(
        graphId, source, direction, roots, limit, USE_DEFINITION_MAX_DEPTH, ReadMode.CACHED);
  }

  @Nonnull
  GraphReadResult expand(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull TraversalDirection direction,
      @Nonnull java.util.Collection<String> roots,
      int limit,
      int maxDepth,
      @Nonnull ReadMode mode);

  /** Resolves a {@link KnownEntityGraph} to {@code { graphId, source }} from loaded config. */
  @Nonnull
  Optional<EntityGraphBinding> bindingForKnownGraph(@Nonnull KnownEntityGraph graph);

  @Nonnull
  Optional<EntityGraphBinding> bindingForFilterField(@Nonnull String searchField);

  @Nonnull
  Optional<EntityGraphBinding> bindingForPolicyField(@Nonnull String fieldType);

  /**
   * Walks ancestors along stored forward edges (e.g. domain {@code IsPartOf} child → parents), up
   * to {@code maxDepth} levels.
   */
  @Nonnull
  AncestorWalkResult walkOrderedForwardAncestors(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seed,
      int maxDepth,
      @Nonnull ReadMode mode);

  /**
   * Relationship aspects indexed for sync invalidation (e.g. {@code domainProperties} → {@code
   * domain} graph). Empty when the cache is disabled or the aspect is not mapped.
   */
  @Nonnull
  default Set<String> getCandidateGraphIds(@Nonnull String entityType, @Nonnull String aspectName) {
    return Collections.emptySet();
  }

  /**
   * Graphs whose configured relationship aspects include {@code entityType}. Used to gate
   * entity-wide (key-aspect) sync deletes without scanning unrelated graphs.
   */
  @Nonnull
  default Set<String> getGraphIdsForEntityType(@Nonnull String entityType) {
    return Collections.emptySet();
  }

  /**
   * Lists typed neighbors of {@code seedUrn} within {@code maxDepth} hops, filtered to {@code
   * relationshipTypes}. Used by the membership graph for actor/group/role walks.
   */
  @Nonnull
  MembershipNeighborResult listRelated(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count,
      @Nonnull ReadMode mode);

  /**
   * Applies status-aware graph cache invalidation after sync inline indexing.
   *
   * <p>See {@code docs/deploy/gms-entity-graph-cache.md} — Invalidation section.
   */
  void invalidateOnSyncBatch(@Nonnull SyncGraphInvalidationBatch batch);
}
