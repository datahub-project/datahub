package com.linkedin.metadata.graph.cache.snapshot;

import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/** Rebuilds {@link EntityGraphSnapshot} metadata from a materialized edge list. */
final class EntityGraphSnapshotMaterializer {

  private EntityGraphSnapshotMaterializer() {}

  @Nonnull
  static EntityGraphSnapshot rebuildWithEdges(
      @Nonnull EntityGraphSnapshot snapshot,
      @Nonnull List<EntityGraphSnapshot.DirectedEdge> edges) {
    Set<String> vertices = new HashSet<>();
    for (EntityGraphSnapshot.DirectedEdge edge : edges) {
      vertices.add(edge.getSourceUrn());
      vertices.add(edge.getDestinationUrn());
    }
    return EntityGraphSnapshot.builder()
        .graphId(snapshot.getGraphId())
        .cacheKey(snapshot.getCacheKey())
        .generation(snapshot.getGeneration())
        .buildSource(snapshot.getBuildSource())
        .builtAtMillis(snapshot.getBuiltAtMillis())
        .edges(edges)
        .vertexCount(vertices.size())
        .edgeCount(edges.size())
        .topologyFingerprint(EntityGraphSnapshotBuilder.topologyFingerprint(edges))
        .traversalCoverage(
            EntityGraphCacheKeys.isFullScopeCacheKey(snapshot.getCacheKey())
                ? TraversalCoverage.fullComplete()
                : TraversalCoverage.incomplete())
        .cacheStatus(CacheStatus.ACTIVE.name())
        .build();
  }

  /**
   * Snapshot for a full-walk write-back. Keeps the caller's cache key and {@code builtAtMillis};
   * coverage is supplied by the caller so the unwalked direction is preserved.
   */
  @Nonnull
  static EntityGraphSnapshot materializeFullWalk(
      @Nonnull String graphId,
      @Nonnull String cacheKey,
      @Nonnull String buildSource,
      long builtAtMillis,
      long generation,
      @Nonnull List<EntityGraphSnapshot.DirectedEdge> edges,
      @Nonnull TraversalCoverage coverage) {
    Set<String> vertices = new HashSet<>();
    for (EntityGraphSnapshot.DirectedEdge edge : edges) {
      vertices.add(edge.getSourceUrn());
      vertices.add(edge.getDestinationUrn());
    }
    return EntityGraphSnapshot.builder()
        .graphId(graphId)
        .cacheKey(cacheKey)
        .generation(generation)
        .buildSource(buildSource)
        .builtAtMillis(builtAtMillis)
        .edges(edges)
        .vertexCount(vertices.size())
        .edgeCount(edges.size())
        .topologyFingerprint(EntityGraphSnapshotBuilder.topologyFingerprint(edges))
        .traversalCoverage(coverage)
        .cacheStatus(CacheStatus.ACTIVE.name())
        .build();
  }
}
