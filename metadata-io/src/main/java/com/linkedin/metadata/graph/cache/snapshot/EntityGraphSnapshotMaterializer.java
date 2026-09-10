package com.linkedin.metadata.graph.cache.snapshot;

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
        .cacheStatus(com.linkedin.metadata.graph.cache.CacheStatus.ACTIVE.name())
        .build();
  }
}
