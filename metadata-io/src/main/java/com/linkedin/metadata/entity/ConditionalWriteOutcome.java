package com.linkedin.metadata.entity;

/**
 * Outcome of {@link AspectDao#saveLatestAspectConditional}. Distinguishes a successful write, a
 * legitimate no-op (do not retry), and a version conflict (retry).
 */
public enum ConditionalWriteOutcome {
  /** Conditional UPDATE matched 1 row. */
  UPDATED,

  /** Aspect + system metadata unchanged; caller must not retry. */
  SKIPPED_NOOP,

  /** Version-0 CAS missed; caller throws {@link OptimisticLockConflictException} to retry. */
  CONFLICT
}
