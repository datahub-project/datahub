package com.linkedin.metadata.buffer.offload;

import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.SizingPolicy;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Store-agnostic async offload buffer: a hot path enqueues work here instead of doing it inline,
 * and a background {@link OffloadDrainer} drains and applies it. The framework provides one
 * Hazelcast implementation, {@link HazelcastOffloadBuffer}; a new offload supplies only the feature
 * bits (key/value types, {@link MergePolicy}, {@link SizingPolicy}, {@link DrainAction}, {@link
 * OffloadContextResolver}) and gets the buffer + drain infra for free — no new "infra keys" (map
 * names, lock maps, scheduling configs) per reuse.
 *
 * <h2>Retention follow-up (NOT in this change)</h2>
 *
 * The framework is designed to also absorb the retention coalesce buffer: {@link
 * MergePolicy#KEEP_MAX_LONG} + {@link SizingPolicy#EVICT_LRU} + {@code V = Long} + a retention
 * {@link DrainAction} ({@code applyRetentionBatch}). That migration is mechanical (same delegation
 * pattern as the post-commit-hook migration) and intentionally left for a follow-up so this change
 * stays small and the hook path ships first.
 *
 * <h2>Contracts</h2>
 *
 * <ul>
 *   <li>{@link #enqueue} — best-effort. With {@link SizingPolicy#REJECT_AT_CAP} it returns {@code
 *       false} when the buffer is at capacity; the caller MUST then run the work synchronously
 *       (bounded memory, no data loss). With {@link SizingPolicy#EVICT_LRU} it always returns
 *       {@code true} and the bound is a Hazelcast eviction policy (latest-wins; eviction = bloat,
 *       not loss).
 *   <li>{@link #nextSequence} — globally-unique monotonic sequence, used to make a key distinct for
 *       {@link MergePolicy#NO_COALESCE} (each committed MCL is a separate fact). Unused for
 *       coalescing uses (keys collide by design).
 *   <li>{@link #drain} — FIFO-ish bounded batch via a {@code PagingPredicate}; only {@code limit}
 *       entries are transferred to the draining member.
 *   <li>{@link #removeIfSame} — CAS clear: removes the entry only if its value still equals {@code
 *       expected}. Guards against a requeued/re-merged entry being clobbered by a stale remove.
 *   <li>{@link #requeue} — re-insert an entry that failed to apply (retry) or a poison pill to
 *       force a retry/DLQ path on the next tick.
 *   <li>Drain lock — {@link #tryAcquireDrainLock}/{@link #releaseDrainLock} are a
 *       <b>non-reentrant</b>, token-fenced lease so two drainer ticks (or two instances) never
 *       drain concurrently and a crashed drainer's lease auto-expires.
 * </ul>
 *
 * @param <K> buffer key type (must be {@link Serializable} for Hazelcast)
 * @param <V> buffer payload type (must be {@link Serializable} for Hazelcast)
 */
public interface OffloadBuffer<K extends Serializable, V extends Serializable> {

  /**
   * Enqueue one work item. Return {@code false} iff the buffer refused the entry because it is at
   * capacity ({@link SizingPolicy#REJECT_AT_CAP}); the caller MUST then run the work inline. Return
   * {@code true} otherwise (including for {@link SizingPolicy#EVICT_LRU}, where the bound is
   * eviction, not a reject).
   */
  boolean enqueue(@Nonnull K key, @Nonnull V value);

  /**
   * Next globally-unique monotonic sequence (for {@link MergePolicy#NO_COALESCE} distinct keys).
   */
  long nextSequence();

  /** Drain up to {@code limit} entries (best-effort bounded batch). */
  @Nonnull
  List<Map.Entry<K, V>> drain(int limit);

  /** CAS clear: remove {@code key} only if its current value still equals {@code expected}. */
  boolean removeIfSame(@Nonnull K key, @Nonnull V expected);

  /** Re-insert an entry (retry path / poison pill). */
  void requeue(@Nonnull K key, @Nonnull V value);

  /** {@code true} iff this buffer actually defers work (the {@code NO_OP} impl returns false). */
  boolean defersApply();

  /** Current number of pending entries (cluster-wide for Hazelcast). */
  int size();

  /**
   * Acquire the non-reentrant drain lock. Returns a fencing token to pass to {@link
   * #releaseDrainLock}, or {@code null} if the lock is held. The lease auto-expires after {@code
   * lease} so a crashed drainer does not wedge the lock.
   */
  @Nullable
  Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease);

  /** Release the drain lock only if the stored token still equals {@code token}. */
  void releaseDrainLock(@Nonnull String lockName, @Nonnull Object token);
}
