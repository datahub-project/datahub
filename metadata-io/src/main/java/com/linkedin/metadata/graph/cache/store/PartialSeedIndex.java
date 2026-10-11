package com.linkedin.metadata.graph.cache.store;

import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/** Pod-local inverted index from vertex URN to PARTIAL snapshot cache keys. */
final class PartialSeedIndex {

  private final Map<String, Map<String, Set<String>>> vertexToKeysByGraphId =
      new ConcurrentHashMap<>();
  private final Map<String, Set<String>> verticesByCacheKey = new ConcurrentHashMap<>();

  void indexSnapshot(@Nonnull EntityGraphSnapshot snapshot) {
    String graphId = snapshot.getGraphId();
    String cacheKey = snapshot.getCacheKey();
    removeCacheKey(cacheKey);
    Set<String> vertices = verticesFromSnapshot(snapshot);
    if (vertices.isEmpty()) {
      return;
    }
    verticesByCacheKey.put(cacheKey, vertices);
    Map<String, Set<String>> vertexToKeys =
        vertexToKeysByGraphId.computeIfAbsent(graphId, ignored -> new ConcurrentHashMap<>());
    for (String vertex : vertices) {
      vertexToKeys.computeIfAbsent(vertex, ignored -> ConcurrentHashMap.newKeySet()).add(cacheKey);
    }
  }

  void removeCacheKey(@Nonnull String cacheKey) {
    Set<String> vertices = verticesByCacheKey.remove(cacheKey);
    if (vertices == null || vertices.isEmpty()) {
      return;
    }
    String graphId = EntityGraphCacheKeys.graphIdFromCacheKey(cacheKey);
    if (graphId == null) {
      return;
    }
    Map<String, Set<String>> vertexToKeys = vertexToKeysByGraphId.get(graphId);
    if (vertexToKeys == null) {
      return;
    }
    for (String vertex : vertices) {
      Set<String> keys = vertexToKeys.get(vertex);
      if (keys == null) {
        continue;
      }
      keys.remove(cacheKey);
      if (keys.isEmpty()) {
        vertexToKeys.remove(vertex);
      }
    }
    if (vertexToKeys.isEmpty()) {
      vertexToKeysByGraphId.remove(graphId);
    }
  }

  void removeGraph(@Nonnull String graphId) {
    Map<String, Set<String>> vertexToKeys = vertexToKeysByGraphId.remove(graphId);
    if (vertexToKeys == null) {
      return;
    }
    for (Set<String> cacheKeys : vertexToKeys.values()) {
      for (String cacheKey : cacheKeys) {
        verticesByCacheKey.remove(cacheKey);
      }
    }
  }

  @Nonnull
  Set<String> candidateKeysForSeeds(
      @Nonnull String graphId, @Nonnull GraphSnapshotSource source, @Nonnull Set<String> seeds) {
    if (seeds.isEmpty()) {
      return Set.of();
    }
    Map<String, Set<String>> vertexToKeys = vertexToKeysByGraphId.get(graphId);
    if (vertexToKeys == null || vertexToKeys.isEmpty()) {
      return Set.of();
    }
    Set<String> candidates = null;
    for (String seed : seeds) {
      Set<String> keysForSeed = vertexToKeys.get(seed);
      if (keysForSeed == null || keysForSeed.isEmpty()) {
        return Set.of();
      }
      Set<String> matching =
          keysForSeed.stream()
              .filter(key -> EntityGraphCacheKeys.cacheKeyMatchesSource(key, graphId, source))
              .collect(java.util.stream.Collectors.toCollection(HashSet::new));
      if (matching.isEmpty()) {
        return Set.of();
      }
      if (candidates == null) {
        candidates = matching;
      } else {
        candidates.retainAll(matching);
        if (candidates.isEmpty()) {
          return Set.of();
        }
      }
    }
    return candidates == null ? Set.of() : Collections.unmodifiableSet(candidates);
  }

  @Nonnull
  private static Set<String> verticesFromSnapshot(@Nonnull EntityGraphSnapshot snapshot) {
    List<EntityGraphSnapshot.DirectedEdge> edges = snapshot.getEdges();
    if (edges == null || edges.isEmpty()) {
      return Set.of();
    }
    Set<String> vertices = new HashSet<>();
    for (EntityGraphSnapshot.DirectedEdge edge : edges) {
      vertices.add(edge.getSourceUrn());
      vertices.add(edge.getDestinationUrn());
    }
    return vertices;
  }
}
