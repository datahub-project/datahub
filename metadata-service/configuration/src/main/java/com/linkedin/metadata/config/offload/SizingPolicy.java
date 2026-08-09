package com.linkedin.metadata.config.offload;

/**
 * How an {@link com.linkedin.metadata.buffer.offload.OffloadBuffer} bounds its pending map. The two
 * strategies reflect the two offload semantics and their data-loss tolerance:
 *
 * <ul>
 *   <li>{@link #REJECT_AT_CAP} — post-commit hooks: each committed MCL is a <b>distinct fact</b>
 *       (the hook reads the MCL's previous-aspect to compute a per-transition delta), so dropping
 *       one loses a side effect. The bound is enforced in {@code enqueue} via a cluster-wide
 *       {@code IMap.size()} check: at cap, {@code enqueue} returns {@code false} and the caller
 *       runs the work synchronously (bounded memory, <b>no data loss</b>). Deliberately NO
 *       {@code EvictionConfig} — eviction would silently drop a distinct committed MCL.
 *   <li>{@link #EVICT_LRU} — retention: semantics are latest-wins, so eviction drops a superseded
 *       prune request (bloat, not loss). The bound is a Hazelcast {@code EvictionConfig}
 *       (PER_NODE + LRU) on the pending {@code MapConfig}; {@code enqueue} does NOT do a
 *       {@code size()} round-trip.
 * </ul>
 *
 * <p>Lives in the configuration module (not {@code metadata-io}) because the use-specific properties
 * subclasses reference it from here, and {@code metadata-io} already depends on this module — the
 * reverse dependency would be a cycle.
 */
public enum SizingPolicy {
  REJECT_AT_CAP,
  EVICT_LRU
}
