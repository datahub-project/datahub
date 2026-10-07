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
  REJECTED_BOUNDS,
  REJECTED_SPLIT_COMPONENTS,
  REJECTED_EMPTY_CREATE
}
