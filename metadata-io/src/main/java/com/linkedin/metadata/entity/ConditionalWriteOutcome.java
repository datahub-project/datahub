package com.linkedin.metadata.entity;

/**
 * Outcome of a conditional (compare-and-set) aspect save. Distinguishes a real write, a legitimate
 * no-op that must not be retried, and a version conflict.
 */
public enum ConditionalWriteOutcome {
  /** Conditional UPDATE matched exactly one row (or a fresh insert succeeded). */
  UPDATED,

  /** Aspect and system metadata are unchanged; nothing was written and the caller must not retry. */
  SKIPPED_NOOP,

  /** The version-0 compare-and-set matched no row; a concurrent writer advanced the version. */
  CONFLICT
}
