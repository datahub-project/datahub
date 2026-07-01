package com.linkedin.metadata.graph.cache;

/** Authoritative store used to build a cached hierarchy snapshot. */
public enum GraphSnapshotSource {
  /**
   * Primary storage aspect lookup ({@code AspectRetriever} / entity service) — per-entity reads of
   * configured relationship fields. Build path supports {@code FORWARD} only.
   */
  PRIMARY,
  /**
   * Primary storage relationship scroll ({@code GraphRetriever} over Ebean/Cassandra). Supports
   * {@code FULL} enumeration or bidirectional {@code PARTIAL} BFS ({@code scope.maxDepth} applies
   * per direction, not summed across directions).
   */
  GRAPH,
  /**
   * Search index scroll over indexed relationship fields. {@code FULL} builds the entire indexed
   * graph; {@code PARTIAL} supports {@code FORWARD} directional BFS from seeds only.
   */
  SEARCH
}
