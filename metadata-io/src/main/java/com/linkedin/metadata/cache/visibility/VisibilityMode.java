package com.linkedin.metadata.cache.visibility;

/** How {@link IndexVisibilityEstimator} bounds the time until a write is searchable. */
public enum VisibilityMode {
  /** Worst case from the configured bulk flush and retry settings plus the refresh interval. */
  SUBMIT_BOUND,
  /** Flush pending writes first. Only the refresh interval is left to wait out. */
  FLUSH_AWAIT
}
