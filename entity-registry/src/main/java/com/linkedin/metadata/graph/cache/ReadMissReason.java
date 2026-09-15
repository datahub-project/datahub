package com.linkedin.metadata.graph.cache;

/** Why a graph cache read could not be satisfied from a materialized snapshot. */
public enum ReadMissReason {
  DISABLED,
  INVALID_REQUEST,
  ABSENT,
  STALE_BLOCKED,
  TOMBSTONE,
  INSUFFICIENT_COVERAGE,
  TRUNCATED
}
