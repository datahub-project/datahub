package com.linkedin.metadata.entity;

/**
 * Outcome of a single aspect write under optimistic locking. Conflict is expressed as data (this
 * enum), never as a control-flow exception, so a batch can collect a per-MCP result instead of
 * aborting on the first conflict.
 */
public enum AspectWriteOutcome {
  /** Conditional UPDATE / insert committed a new value. */
  COMMITTED,

  /** Aspect + system metadata unchanged; nothing written. Must not be retried. */
  NOOP,

  /** Version-0 CAS missed (concurrent writer advanced the version). Eligible for scoped retry. */
  CONFLICT,

  /**
   * Non-retryable failure (write disabled, validation). Distinct from {@link #CONFLICT}: retrying
   * cannot succeed, so it routes to the failed-result / DLQ path rather than the retry loop.
   */
  FAILED
}
