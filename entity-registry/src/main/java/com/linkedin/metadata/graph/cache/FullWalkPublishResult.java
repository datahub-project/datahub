package com.linkedin.metadata.graph.cache;

/** Outcome of {@link EntityGraphCache#publishFullWalk}. */
public enum FullWalkPublishResult {
  PUBLISHED,
  REJECTED_DISABLED,
  REJECTED_SIGNAL,
  REJECTED_TRUNCATED,
  REJECTED_NOT_PARTIAL,
  REJECTED_SOURCE,
  REJECTED_GENERATION,

  /** The walk started before the stored component was built, so it would move the cache back. */
  REJECTED_OUTDATED,

  /**
   * The graph defers invalidation, a write became searchable recently, and the write-back did not
   * say when the walk started, so it may predate that write.
   */
  REJECTED_START_UNKNOWN,
  REJECTED_BOUNDS,
  REJECTED_SPLIT_COMPONENTS,
  REJECTED_EMPTY_CREATE
}
