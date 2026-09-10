package com.linkedin.metadata.graph.cache;

public enum CacheStatus {
  ACTIVE,
  /** Transient build failure; rebuild allowed after population.intervalSeconds. */
  COOLDOWN,
  /**
   * Bounds exceeded with no publishable snapshot; no automatic rebuild until invalidation or config
   * change.
   */
  OVER_LIMIT,
  /**
   * Unsupported build request or invalid graph configuration; no automatic rebuild until
   * invalidation or config change.
   */
  INVALID,
  /** Rebuild lease held in {@code entityGraphStatus} while a pod is building this cache key. */
  BUILDING,
  /** No {@code entityGraphStatus} entry and no published snapshot for this key (cold miss). */
  ABSENT
}
