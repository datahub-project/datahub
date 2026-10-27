package com.linkedin.metadata.graph.cache;

/** BFS direction over stored snapshot edges (source → destination). */
public enum TraversalDirection {
  /** Follow stored directed edges (source → destination). */
  FORWARD,
  /** Traverse against stored edge direction. */
  REVERSE
}
