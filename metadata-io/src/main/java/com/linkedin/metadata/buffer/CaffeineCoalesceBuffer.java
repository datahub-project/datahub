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
import java.util.concurrent.locks.ReentrantLock;
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
 * <p>Drain locks are local {@link ReentrantLock}s keyed by lock name; they coordinate drainer
 * threads within this JVM only and are not cluster-wide. {@code lease} is accepted for API parity
 * with distributed backends but is not enforced here — a single JVM losing a thread mid-drain
 * without unlocking is a bug to fix, not a multi-pod race to bound with a timeout.
 */
@Slf4j
public class CaffeineCoalesceBuffer<K, V> implements CoalesceBuffer<K, V> {

  private final ConcurrentMap<K, V> map;
  private final ConcurrentMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
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
    return locks.computeIfAbsent(lockName, k -> new ReentrantLock()).tryLock();
  }

  @Override
  public void releaseDrainLock(@Nonnull String lockName) {
    ReentrantLock lock = locks.get(lockName);
    if (lock != null && lock.isHeldByCurrentThread()) {
      lock.unlock();
    } else {
      log.warn("Attempted to release coalesce buffer drain lock '{}' that was not held", lockName);
    }
  }
}
