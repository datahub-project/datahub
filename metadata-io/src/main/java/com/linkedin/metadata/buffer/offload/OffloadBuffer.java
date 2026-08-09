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

  /**
   * Allocate the next {@code count} sequence numbers in one cluster call and return the highest.
   * The caller consumes {@code [highest - count + 1 .. highest]} in order. The default impl loops
   * {@link #nextSequence()}; {@link HazelcastOffloadBuffer} overrides with one {@code
   * IAtomicLong.addAndGet} so a batch of N MCLs costs one Raft round-trip, not N. Passing {@code
   * count <= 0} returns {@code 0} without a cluster call.
   */
  default long nextSequence(int count) {
    long last = 0L;
    for (int i = 0; i < count; i++) {
      last = nextSequence();
    }
    return last;
  }

  /** Drain up to {@code limit} entries (best-effort bounded batch). */
  @Nonnull
  List<Map.Entry<K, V>> drain(int limit);

  /** CAS clear: remove {@code key} only if its current value still equals {@code expected}. */
  boolean removeIfSame(@Nonnull K key, @Nonnull V expected);

  /**
   * Re-insert an entry (retry path / poison pill / transient-backoff re-merge). Applies the use's
   * merge policy <b>synchronously</b> (blocks until the entry is visible in the buffer), unlike
   * {@link #enqueue} which is fire-and-forget on the ingest hot path. Callers run on the background
   * drainer thread, so blocking is correct and affordable — and required for the drainer's backoff
   * re-merge, where the re-merged entry must be visible to the {@link #drain} on the same tick.
   * {@link HazelcastOffloadBuffer}: NO_COALESCE → plain {@code put} (unique key, updated value);
   * KEEP_MAX_LONG → synchronous {@code executeOnKey} keep-max merge (coalesces with any newer
   * version, never clobbers a higher one).
   */
  void requeue(@Nonnull K key, @Nonnull V value);

  /**
   * Remove a batch of entries in one cluster call. <b>Non-CAS in the Hazelcast impl</b> ({@code
   * IMap.removeAll(keys)} removes the keys unconditionally, unlike {@link #removeIfSame}'s
   * compare-and-swap). The caller MUST guarantee no {@link #requeue} of any of these keys between
   * {@link #drain} and this call — otherwise a requeued entry (new value) would be silently
   * clobbered. Safe call sites are the success / permanent-drop branches of a {@link DrainAction}
   * and the drainer's own permanent/backoff drops, where no requeue of these keys occurs in the
   * same tick. The retry path (remove-then-requeue on the same key) MUST keep per-entry {@link
   * #removeIfSame}. Default impl loops {@link #removeIfSame} (CAS, correct for tests/NO_OP); {@link
   * HazelcastOffloadBuffer} overrides with one {@code IMap.removeAll}.
   */
  default void removeAll(@Nonnull List<Map.Entry<K, V>> entries) {
    for (Map.Entry<K, V> e : entries) {
      removeIfSame(e.getKey(), e.getValue());
    }
  }

  /**
   * Enqueue a batch in one cluster call. Returns {@code false} iff the buffer rejected the whole
   * batch (at capacity, or transient failure) — the caller MUST then run every entry synchronously
   * (no data loss). All-or-nothing: either every entry is admitted or none and the caller falls
   * back for all. The default impl loops {@link #enqueue}; {@link HazelcastOffloadBuffer} overrides
   * with one {@code IMap.putAll} (NO_COALESCE) so a batch of N costs one round-trip, not N.
   */
  default boolean enqueueBatch(@Nonnull List<Map.Entry<K, V>> entries) {
    boolean allOk = true;
    for (Map.Entry<K, V> e : entries) {
      if (!enqueue(e.getKey(), e.getValue())) {
        allOk = false;
      }
    }
    return allOk;
  }

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
