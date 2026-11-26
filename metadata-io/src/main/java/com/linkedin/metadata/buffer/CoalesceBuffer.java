package com.linkedin.metadata.buffer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Store-agnostic coalescing buffer: callers merge repeated writes for the same key into a single
 * pending value, then a background drainer periodically {@link #drain}s bounded batches and applies
 * them. Backed by a distributed store (Hazelcast today; Redis/Dragonfly reserved for later) so
 * callers never depend on the backing technology (no {@code IMap} or Redis types on this API).
 *
 * @param <K> coalescing key type
 * @param <V> pending value type
 */
public interface CoalesceBuffer<K, V> {

  /**
   * Merges {@code value} into any existing pending value for {@code key} using {@code merge}. If no
   * value is currently pending for {@code key}, {@code value} is stored directly ({@code merge} is
   * not invoked). Implementations may drop the merge (accounted as overflow) when the buffer is at
   * capacity and {@code key} is not already present; existing keys are never dropped, only new ones
   * — missed coalescing is bloat, not data loss.
   */
  void merge(@Nonnull K key, @Nonnull V value, @Nonnull BinaryOperator<V> merge);

  /**
   * Returns a snapshot copy of up to {@code limit} pending entries. Not a consistent point-in-time
   * view for distributed backends, but sufficient for a best-effort bounded drain batch.
   */
  @Nonnull
  List<Map.Entry<K, V>> drain(int limit);

  /**
   * Removes {@code key} only if its current pending value still equals {@code expected}, so a
   * concurrent merge that landed while a drainer was working on {@code key} is not lost.
   */
  boolean removeIfSame(@Nonnull K key, @Nonnull V expected);

  /**
   * Attempts to acquire the named drain lock without blocking. On success returns a non-null
   * fencing token that must be passed to {@link #releaseDrainLock} in a {@code finally} block;
   * returns {@code null} if the lock is already held. {@code lease} is a safety-net so a drainer
   * that dies mid-drain doesn't wedge the lock forever — once it expires another caller may
   * acquire, which is why release is token-fenced.
   */
  @Nullable
  Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease);

  /**
   * Releases the named drain lock only if {@code token} (from {@link #tryAcquireDrainLock}) still
   * matches the current holder. No-op (with a warning log) if the lease already expired and another
   * caller re-acquired — this never clears a lock the caller no longer owns.
   */
  void releaseDrainLock(@Nonnull String lockName, @Nonnull Object token);
}
