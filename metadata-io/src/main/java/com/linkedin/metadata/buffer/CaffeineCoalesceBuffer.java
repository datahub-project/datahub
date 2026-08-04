package com.linkedin.metadata.buffer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Local, single-JVM {@link CoalesceBuffer} backed by a Caffeine {@link Cache}. No cross-pod
 * coalescing: each GMS pod has its own independent buffer.
 *
 * <p>Deliberately built with no {@code maximumSize}/eviction policy on the Caffeine cache itself —
 * silent Caffeine-driven eviction would drop pending entries without the explicit "bloat, not loss"
 * overflow accounting this class provides for new keys, so capacity is enforced manually in {@link
 * #merge} instead (existing keys are never evicted, only new keys are rejected once full).
 *
 * <p>Drain locks are local, non-reentrant, and lease-based: each holds an expiry timestamp keyed by
 * lock name, coordinating drainer threads within this JVM only (not cluster-wide). The {@code
 * lease} is enforced — a drainer that dies mid-drain without releasing cannot wedge the lock past
 * its lease (stuck-lock recovery, mirroring the Hazelcast backend's TTL lock).
 */
@Slf4j
public class CaffeineCoalesceBuffer<K, V> implements CoalesceBuffer<K, V> {

  private final ConcurrentMap<K, V> map;
  // Value = lease-expiry epoch millis; 0 means free.
  private final ConcurrentMap<String, AtomicLong> lockExpiries = new ConcurrentHashMap<>();
  private final int maxPendingEntries;
  private final String name;
  @Nullable private final MetricUtils metricUtils;

  public CaffeineCoalesceBuffer(
      @Nonnull String name, int maxPendingEntries, @Nullable MetricUtils metricUtils) {
    this.name = name;
    this.maxPendingEntries = maxPendingEntries;
    this.metricUtils = metricUtils;
    Cache<K, V> cache = Caffeine.newBuilder().build();
    this.map = cache.asMap();
  }

  @Override
  public void merge(@Nonnull K key, @Nonnull V value, @Nonnull BinaryOperator<V> merge) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(value, "value must not be null");
    Objects.requireNonNull(merge, "merge must not be null");
    // Best-effort soft cap: a race between size() and merge() can let the map briefly exceed
    // maxPendingEntries under concurrent callers. Acceptable since this only bounds bloat risk,
    // not correctness.
    if (map.size() >= maxPendingEntries && !map.containsKey(key)) {
      if (metricUtils != null) {
        metricUtils.increment(CaffeineCoalesceBuffer.class, name + "_overflow", 1);
      }
      log.debug(
          "Coalesce buffer '{}' full ({} entries); dropping merge for key={}",
          name,
          maxPendingEntries,
          key);
      return;
    }
    map.merge(key, value, merge);
  }

  @Override
  @Nonnull
  public List<Map.Entry<K, V>> drain(int limit) {
    List<Map.Entry<K, V>> batch = new ArrayList<>(Math.min(limit, 64));
    for (Map.Entry<K, V> entry : map.entrySet()) {
      if (batch.size() >= limit) {
        break;
      }
      batch.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue()));
    }
    return batch;
  }

  @Override
  public boolean removeIfSame(@Nonnull K key, @Nonnull V expected) {
    return map.remove(key, expected);
  }

  @Override
  public boolean tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
    long now = System.currentTimeMillis();
    long newExpiry = now + Math.max(1L, lease.toMillis());
    AtomicLong holder = lockExpiries.computeIfAbsent(lockName, k -> new AtomicLong(0L));
    while (true) {
      long current = holder.get();
      // Held and not yet expired → fail (non-reentrant: same thread cannot re-acquire).
      if (current != 0L && now < current) {
        return false;
      }
      // Free, or the prior holder's lease expired → steal it (stuck-lock recovery).
      if (holder.compareAndSet(current, newExpiry)) {
        return true;
      }
    }
  }

  @Override
  public void releaseDrainLock(@Nonnull String lockName) {
    AtomicLong holder = lockExpiries.get(lockName);
    if (holder == null || holder.getAndSet(0L) == 0L) {
      log.warn("Attempted to release coalesce buffer drain lock '{}' that was not held", lockName);
    }
  }
}
