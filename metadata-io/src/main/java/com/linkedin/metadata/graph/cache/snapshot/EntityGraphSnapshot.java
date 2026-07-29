package com.linkedin.metadata.graph.cache.snapshot;

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class EntityGraphSnapshot implements Serializable {
  private static final long serialVersionUID = 3L;

  String graphId;
  String cacheKey;
  long generation;
  String buildSource;
  long builtAtMillis;
  List<DirectedEdge> edges;
  int vertexCount;
  int edgeCount;
  String topologyFingerprint;
  @Nullable TraversalCoverage traversalCoverage;

  /**
   * {@link com.linkedin.metadata.graph.cache.CacheStatus#name()} when published; null in builders.
   */
  @Nullable String cacheStatus;

  @Value
  @Builder
  public static class DirectedEdge implements Serializable {
    private static final long serialVersionUID = 1L;
    String sourceUrn;
    String destinationUrn;
    String relationshipType;

    @Nonnull
    public String canonicalLine() {
      return sourceUrn + "->" + destinationUrn + ":" + relationshipType;
    }
  }
}
