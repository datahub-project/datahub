package com.linkedin.metadata.entity;

/**
 * Outcome of a single CAS update within a batch. Used by {@link
 * AspectDao#updateAspectsConditionalBatch}. Result ordering matches input ordering exactly.
 */
public enum ConditionalUpdateResult {
  /** Conditional UPDATE matched 1 row (affectedRows == 1). */
  UPDATED,

  /** Version-0 CAS missed (affectedRows == 0); legitimate conflict, not an error. */
  CONFLICT
}
