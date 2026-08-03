package com.linkedin.metadata.buffer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicates;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Hazelcast-backed {@link CoalesceBuffer} with value type fixed to {@link Long}: coalesces merges
 * into a shared {@code IMap<K, Long>} ({@code name}), with a second {@code IMap<String, Boolean>}
 * ({@code lockMapName}) used as a <b>non-reentrant</b> drain lock via {@code putIfAbsent} (not
 * {@code IMap.tryLock}, which is re-entrant for the same thread).
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

  private final IMap<K, Long> pendingMap;
  private final IMap<String, Boolean> lockMap;

  public HazelcastCoalesceBuffer(
      @Nonnull HazelcastInstance hazelcastInstance,
      @Nonnull String name,
      @Nonnull String lockMapName) {
    this.pendingMap = hazelcastInstance.getMap(name);
    this.lockMap = hazelcastInstance.getMap(lockMapName);
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
    // Fire-and-forget: this runs on the ingest request thread (post-commit). executeOnKeyAsync so a
    // partitioned/GC-paused cluster cannot stall the ingest response for the Hazelcast op timeout —
    // a dropped merge is under-coalescing (bloat), never data loss. No size()/containsKey() RTT.
    pendingMap.executeOnKeyAsync(key, new KeepMaxLongProcessor<>(value));
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
  public boolean tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
    // Non-reentrant: IMap.tryLock is re-entrant for the same thread, which breaks the mutual-
    // exclusion contract the drain tests (and multi-drainer same-thread edge cases) rely on.
    // putIfAbsent fails if the key is already present, even for this thread. TTL ≈ lease so a
    // crashed drainer does not wedge the lock forever.
    long leaseSeconds = Math.max(1, lease.getSeconds());
    return lockMap.putIfAbsent(lockName, Boolean.TRUE, leaseSeconds, TimeUnit.SECONDS) == null;
  }

  @Override
  public void releaseDrainLock(@Nonnull String lockName) {
    if (!lockMap.remove(lockName, Boolean.TRUE)) {
      log.warn("Attempted to release coalesce buffer drain lock '{}' that was not held", lockName);
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
