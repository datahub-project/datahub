package com.linkedin.metadata.buffer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import javax.annotation.Nonnull;

/**
 * Store-agnostic coalescing buffer: callers merge repeated writes for the same key into a single
 * pending value, then a background drainer periodically {@link #drain}s bounded batches and applies
 * them. Implementations back this with a local cache (Caffeine) or a distributed store (Hazelcast
 * today; Redis/Dragonfly reserved for later) so callers never depend on the backing technology (no
 * {@code IMap}, Caffeine, or Redis types on this API).
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
   * Attempts to acquire the named drain lock without blocking. Callers must release it via {@link
   * #releaseDrainLock} in a {@code finally} block when this returns {@code true}. {@code lease} is
   * a safety-net so a drainer that dies mid-drain doesn't wedge the lock forever; local
   * (non-distributed) implementations may not enforce it — see the implementation's javadoc.
   */
  boolean tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease);

  /** Releases the named drain lock. No-op (aside from a warning log) if not held by the caller. */
  void releaseDrainLock(@Nonnull String lockName);
}
