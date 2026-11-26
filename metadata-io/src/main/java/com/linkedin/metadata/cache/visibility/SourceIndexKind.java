package com.linkedin.metadata.cache.visibility;

/**
 * Index a cache is built from. A synchronous write reaches either one through an async bulk
 * processor, and neither shows the write until its {@code refresh_interval} has passed.
 */
public enum SourceIndexKind {
  /** The relationship (graph) index. */
  GRAPH,
  /** The entity search index. */
  ENTITY
}
