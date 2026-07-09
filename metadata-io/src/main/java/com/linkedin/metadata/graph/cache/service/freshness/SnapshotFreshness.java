package com.linkedin.metadata.graph.cache.service.freshness;

/** Unified freshness verdict for a distributed cache entry. */
public enum SnapshotFreshness {
  /** Snapshot is within interval and satisfies direction coverage — serve reads. */
  FRESH,
  /** ACTIVE but past interval; serve under BUILDING lease, block under BACKGROUND rebuild. */
  STALE_SERVABLE,
  /** Fail-closed for cached reads (BACKGROUND rebuild in flight or cold miss after staleness). */
  STALE_BLOCKED,
  /** COOLDOWN, OVER_LIMIT, or INVALID — no serve; rebuild policy depends on tombstone type. */
  TOMBSTONE,
  /** No published snapshot for this key. */
  ABSENT
}
