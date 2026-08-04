package com.linkedin.metadata.buffer;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Local, single-JVM {@link CoalesceBuffer} backed by a plain {@link ConcurrentHashMap}. No
 * cross-pod coalescing: each pod has its own independent buffer. ("caffeine" is the config-facing
 * backend id in {@code datahub.buffer.implementation}; no Caffeine eviction features are needed
 * here.)
 *
 * <p>Capacity is enforced manually in {@link #merge} rather than via a cache eviction policy, so an
 * over-capacity insert is accounted as an explicit "bloat, not loss" overflow drop — existing keys
 * are never evicted, only new keys are rejected once full.
 *
 * <p>Drain locks are local, non-reentrant, and lease-based: each is keyed by lock name and holds a
 * lease (expiry + a unique fencing token), coordinating drainer threads within this JVM only (not
 * cluster-wide). The {@code lease} is enforced (stuck-lock recovery) and release is token-fenced —
 * a drainer whose lease expired cannot clear the lock a later drainer re-acquired (mirroring the
 * Hazelcast backend's TTL lock).
 */
@Slf4j
public class LocalCoalesceBuffer<K, V> implements CoalesceBuffer<K, V> {

  private final ConcurrentMap<K, V> map;
  // Drain locks: name -> current lease (expiry for stuck-lock recovery + a globally-unique token so
  // release is fenced and a stale holder can never clear a later holder's lock).
  private final ConcurrentMap<String, AtomicReference<Lease>> locks = new ConcurrentHashMap<>();
  private final AtomicLong tokenSeq = new AtomicLong();
  private final int maxPendingEntries;
  private final String name;
  @Nullable private final MetricUtils metricUtils;

  public LocalCoalesceBuffer(
      @Nonnull String name, int maxPendingEntries, @Nullable MetricUtils metricUtils) {
    this.name = name;
    this.maxPendingEntries = maxPendingEntries;
    this.metricUtils = metricUtils;
    this.map = new ConcurrentHashMap<>();
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
        metricUtils.increment(LocalCoalesceBuffer.class, name + "_overflow", 1);
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
    // ConcurrentHashMap iteration order is unspecified, so the bounded batch a pod picks is
    // non-deterministic (unlike the Hazelcast backend's ordered PagingPredicate). Fine: drain is
    // best-effort and every pod drains its own local buffer, so batch composition doesn't matter.
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
  @Nullable
  public Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
    long now = System.currentTimeMillis();
    AtomicReference<Lease> holder = locks.computeIfAbsent(lockName, k -> new AtomicReference<>());
    while (true) {
      Lease current = holder.get();
      // Held and not yet expired → fail (non-reentrant: same thread cannot re-acquire).
      if (current != null && now < current.expiryMs()) {
        return null;
      }
      // Free, or the prior holder's lease expired → steal it (stuck-lock recovery). The token is a
      // globally-unique sequence, so a stale holder's release can never match a later holder's lock
      // regardless of clock values.
      Lease next = new Lease(now + Math.max(1L, lease.toMillis()), tokenSeq.incrementAndGet());
      if (holder.compareAndSet(current, next)) {
        return next.token();
      }
    }
  }

  @Override
  public void releaseDrainLock(@Nonnull String lockName, @Nonnull Object token) {
    AtomicReference<Lease> holder = locks.get(lockName);
    Lease current = holder == null ? null : holder.get();
    // Contract: `token` is exactly the opaque value returned by THIS buffer's tryAcquireDrainLock,
    // so it is always the Long fencing token minted above (a caller never fabricates or crosses
    // tokens between backends). Clear only if our token still owns the lock; if the lease expired
    // and another drainer re-acquired (a new token), leave theirs intact.
    if (current == null
        || current.token() != (Long) token
        || !holder.compareAndSet(current, null)) {
      log.warn(
          "Drain lock '{}' not released by owner — lease likely expired and it was re-acquired",
          lockName);
    }
  }

  private record Lease(long expiryMs, long token) {}
}
