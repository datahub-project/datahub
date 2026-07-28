package com.linkedin.metadata.buffer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.Serializable;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Hazelcast-backed {@link CoalesceBuffer} with value type fixed to {@link Long}: coalesces merges
 * into a shared {@code IMap<K, Long>} ({@code name}), with a second {@code IMap<String, Boolean>}
 * ({@code lockMapName}) used purely as a distributed lock namespace for {@link
 * #tryAcquireDrainLock}/{@link #releaseDrainLock}.
 *
 * <p><b>Merge policy constraint:</b> Hazelcast {@link EntryProcessor}s must be serialized to run on
 * the owning cluster member, so this class cannot ship an arbitrary {@link BinaryOperator} over the
 * wire. Only {@link CoalesceBuffers#KEEP_MAX_LONG} is supported today (checked by reference
 * identity, since it's a fixed static constant); any other merge function throws {@link
 * UnsupportedOperationException}. Supporting additional policies would need {@code
 * IdentifiedDataSerializable} merge processors registered with Hazelcast's serialization config —
 * not implemented in this change.
 *
 * <p>Sizing note: no {@code hazelcast.xml}/{@code .yaml} config file exists in this repo — maps are
 * configured programmatically via {@code MapConfig} beans injected into the shared {@code
 * HazelcastInstance} (see {@code CacheConfig} / {@code RetentionBufferFactory}), which can register
 * a bounded eviction policy for {@code name} as a second line of defense behind the {@code
 * maxPendingEntries} soft-cap enforced here.
 */
@Slf4j
public class HazelcastCoalesceBuffer<K> implements CoalesceBuffer<K, Long> {

  private final IMap<K, Long> pendingMap;
  private final IMap<String, Boolean> lockMap;
  private final int maxPendingEntries;
  private final String name;
  @Nullable private final MetricUtils metricUtils;

  public HazelcastCoalesceBuffer(
      @Nonnull HazelcastInstance hazelcastInstance,
      @Nonnull String name,
      @Nonnull String lockMapName,
      int maxPendingEntries,
      @Nullable MetricUtils metricUtils) {
    this.name = name;
    this.pendingMap = hazelcastInstance.getMap(name);
    this.lockMap = hazelcastInstance.getMap(lockMapName);
    this.maxPendingEntries = maxPendingEntries;
    this.metricUtils = metricUtils;
  }

  @Override
  public void merge(@Nonnull K key, @Nonnull Long value, @Nonnull BinaryOperator<Long> merge) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(value, "value must not be null");
    Objects.requireNonNull(merge, "merge must not be null");
    if (merge != CoalesceBuffers.KEEP_MAX_LONG) {
      throw new UnsupportedOperationException(
          "HazelcastCoalesceBuffer only supports CoalesceBuffers.KEEP_MAX_LONG until "
              + "IdentifiedDataSerializable merge policies exist");
    }
    // Best-effort soft cap: a race between size() and executeOnKey can let the map briefly
    // exceed maxPendingEntries under concurrent callers. Acceptable since this only bounds
    // bloat risk, not correctness.
    if (pendingMap.size() >= maxPendingEntries && !pendingMap.containsKey(key)) {
      if (metricUtils != null) {
        metricUtils.increment(HazelcastCoalesceBuffer.class, name + "_overflow", 1);
      }
      log.debug(
          "Coalesce buffer '{}' full ({} entries); dropping merge for key={}",
          name,
          maxPendingEntries,
          key);
      return;
    }
    pendingMap.executeOnKey(key, new KeepMaxLongProcessor<>(value));
  }

  @Override
  @Nonnull
  public List<Map.Entry<K, Long>> drain(int limit) {
    List<Map.Entry<K, Long>> batch = new ArrayList<>(Math.min(limit, 64));
    for (Map.Entry<K, Long> entry : pendingMap.entrySet()) {
      if (batch.size() >= limit) {
        break;
      }
      batch.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue()));
    }
    return batch;
  }

  @Override
  public boolean removeIfSame(@Nonnull K key, @Nonnull Long expected) {
    return pendingMap.remove(key, expected);
  }

  @Override
  public boolean tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
    try {
      long leaseSeconds = Math.max(1, lease.getSeconds());
      return lockMap.tryLock(lockName, 0, TimeUnit.SECONDS, leaseSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  @Override
  public void releaseDrainLock(@Nonnull String lockName) {
    try {
      lockMap.unlock(lockName);
    } catch (IllegalMonitorStateException e) {
      log.warn(
          "Attempted to release coalesce buffer drain lock '{}' that was not held", lockName, e);
    }
  }

  /**
   * Keep-max coalescing {@link EntryProcessor} for a single key. {@code EntryProcessor} already
   * extends {@link Serializable}; Hazelcast serializes this instance to run on the owning member.
   */
  static final class KeepMaxLongProcessor<K> implements EntryProcessor<K, Long, Void> {
    private static final long serialVersionUID = 1L;

    private final long candidateMaxVersion;

    KeepMaxLongProcessor(long candidateMaxVersion) {
      this.candidateMaxVersion = candidateMaxVersion;
    }

    @Override
    public Void process(Map.Entry<K, Long> entry) {
      Long current = entry.getValue();
      if (current == null || candidateMaxVersion > current) {
        entry.setValue(candidateMaxVersion);
      }
      return null;
    }
  }
}
