package com.linkedin.metadata.config.offload;

/**
 * Coalescing strategy for an {@link com.linkedin.metadata.buffer.offload.OffloadBuffer}'s {@code
 * enqueue}. The framework dispatches on this rather than accepting an arbitrary {@code
 * BinaryOperator<V>} merge function because a Hazelcast {@code EntryProcessor} must be serialized
 * to run on the owning cluster member — a plain lambda merge cannot ship over the wire. The two
 * built-ins cover the two known uses:
 *
 * <ul>
 *   <li>{@link #NO_COALESCE} — post-commit hooks: keys are globally unique (via {@code
 *       nextSequence()}), so {@code enqueue} is a plain {@code put}; no merge ever fires. Pair with
 *       {@link SizingPolicy#REJECT_AT_CAP} (no-loss bound).
 *   <li>{@link #KEEP_MAX_LONG} — retention: coalesces {@code Long} values via a serializable {@code
 *       KeepMaxLongProcessor} entry processor. Pair with {@link SizingPolicy#EVICT_LRU}
 *       (latest-wins; eviction = bloat, not loss).
 * </ul>
 *
 * <p>Adding a new coalescing policy requires a serializable {@code EntryProcessor} registered with
 * Hazelcast's serialization config — not extensible via a lambda.
 *
 * <p>Lives in the configuration module (not {@code metadata-io}) because the use-specific
 * properties subclasses extend {@link OffloadBufferProperties} from here, and {@code metadata-io}
 * already depends on this module — the reverse dependency would be a cycle.
 */
public enum MergePolicy {
  NO_COALESCE,
  KEEP_MAX_LONG
}
