package com.linkedin.metadata.buffer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicates;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Hazelcast-backed {@link CoalesceBuffer} with value type fixed to {@link Long}: coalesces merges
 * into a shared {@code IMap<K, Long>} ({@code name}), with a second {@code IMap<String, String>}
 * ({@code lockMapName}) used as a <b>non-reentrant</b>, token-fenced drain lock via {@code
 * putIfAbsent} (not {@code IMap.tryLock}, which is re-entrant for the same thread).
 *
 * <p><b>Merge policy constraint:</b> Hazelcast {@link EntryProcessor}s must be serialized to run on
 * the owning cluster member, so this class cannot ship an arbitrary {@link BinaryOperator} over the
 * wire. Only {@link CoalesceBuffers#KEEP_MAX_LONG} is supported today (checked by reference
 * identity, since it's a fixed static constant); any other merge function throws {@link
 * UnsupportedOperationException}. Supporting additional policies would need {@code
 * IdentifiedDataSerializable} merge policies registered with Hazelcast's serialization config — not
 * implemented in this change.
 *
 * <p><b>Sizing:</b> no per-merge {@code size()}/{@code containsKey()} soft-cap on the ingest path
 * (those are cluster round-trips). Capacity is enforced by the bounded {@code MapConfig} registered
 * for {@code name} (see {@code RetentionBufferFactory} / {@code CacheConfig}). Missed prune under
 * eviction = storage bloat, not data loss.
 *
 * <p><b>Drain:</b> uses a {@link PagingPredicate} so only {@code limit} entries are transferred to
 * the draining member, not the full map.
 */
@Slf4j
public class HazelcastCoalesceBuffer<K> implements CoalesceBuffer<K, Long> {

  // Fail-fast bound on the per-merge cluster op so a partitioned / GC-paused member cannot stall
  // the
  // ingest thread for Hazelcast's (5-minute) default operation timeout. On timeout the merge is
  // dropped (best-effort: under-coalescing = bloat, never data loss).
  private static final long MERGE_TIMEOUT_MS = 1000L;

  private final IMap<K, Long> pendingMap;
  // Value is a per-acquire fencing token (UUID), not a flag, so releaseDrainLock only clears the
  // lock the caller still owns.
  private final IMap<String, String> lockMap;
  @Nullable private final MetricUtils metricUtils;

  public HazelcastCoalesceBuffer(
      @Nonnull HazelcastInstance hazelcastInstance,
      @Nonnull String name,
      @Nonnull String lockMapName,
      @Nullable MetricUtils metricUtils) {
    this.pendingMap = hazelcastInstance.getMap(name);
    this.lockMap = hazelcastInstance.getMap(lockMapName);
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
    // Bounded synchronous apply: submitToKey runs the entry processor on the owning member and we
    // wait up to MERGE_TIMEOUT_MS. Waiting keeps the merge observable to a drain that reads right
    // after (the void CoalesceBuffer#merge contract) without the unbounded stall a plain
    // executeOnKey would incur under a partition; a fire-and-forget
    // submitToKey would instead race the drain and drop coalesced updates. On timeout/failure the
    // merge is dropped and metric'd — best-effort, bloat not loss. No size()/containsKey() RTT.
    try {
      pendingMap
          .submitToKey(key, new KeepMaxLongProcessor<>(value))
          .toCompletableFuture()
          .get(MERGE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      incrementDrop("retention_buffer_merge_timeout");
    } catch (ExecutionException e) {
      incrementDrop("retention_buffer_merge_failed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      incrementDrop("retention_buffer_merge_interrupted");
    }
  }

  private void incrementDrop(@Nonnull String metric) {
    log.debug("Hazelcast retention merge dropped ({})", metric);
    if (metricUtils != null) {
      metricUtils.increment(HazelcastCoalesceBuffer.class, metric, 1);
    }
  }

  @Override
  @Nonnull
  public List<Map.Entry<K, Long>> drain(int limit) {
    if (limit <= 0) {
      return List.of();
    }
    // Explicit comparator so paging does NOT fall back to natural ordering, which would cast the
    // key to Comparable and throw ClassCastException for non-Comparable keys (e.g. RetentionKey).
    // Any deterministic total order is fine here — drain is a best-effort bounded batch, not a
    // ranked query. Comparator must be Serializable (it ships to cluster members).
    PagingPredicate<K, Long> page = Predicates.pagingPredicate(new DrainOrder<>(), limit);
    return new ArrayList<>(pendingMap.entrySet(page));
  }

  @Override
  public boolean removeIfSame(@Nonnull K key, @Nonnull Long expected) {
    return pendingMap.remove(key, expected);
  }

  @Override
  @Nullable
  public Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
    // Non-reentrant: IMap.tryLock is re-entrant for the same thread; putIfAbsent fails if the key
    // is
    // present even for this thread. The stored value is a per-acquire fencing token so release only
    // clears our own lock. TTL = lease (millisecond granularity, matching the local backend) so a
    // crashed drainer does not wedge the lock forever.
    String token = UUID.randomUUID().toString();
    long leaseMillis = Math.max(1L, lease.toMillis());
    boolean acquired =
        lockMap.putIfAbsent(lockName, token, leaseMillis, TimeUnit.MILLISECONDS) == null;
    return acquired ? token : null;
  }

  @Override
  public void releaseDrainLock(@Nonnull String lockName, @Nonnull Object token) {
    // Remove only if the stored token is still ours; if the lease expired and another drainer
    // re-acquired (new token), this no-ops and leaves their lock intact.
    if (!lockMap.remove(lockName, token)) {
      log.warn(
          "Drain lock '{}' not released by owner — lease likely expired and it was re-acquired",
          lockName);
    }
  }

  /**
   * Serializable total order for {@link #drain}'s {@link PagingPredicate}. Orders by pending value
   * then key string so paging never relies on the key being {@link Comparable}. Order is arbitrary
   * (drain is best-effort), only stability/totality matter.
   */
  static final class DrainOrder<K> implements Comparator<Map.Entry<K, Long>>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Map.Entry<K, Long> a, Map.Entry<K, Long> b) {
      int byValue = Long.compare(a.getValue(), b.getValue());
      if (byValue != 0) {
        return byValue;
      }
      return String.valueOf(a.getKey()).compareTo(String.valueOf(b.getKey()));
    }
  }

  /** Keep-max coalescing {@link EntryProcessor} for a single key. */
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
