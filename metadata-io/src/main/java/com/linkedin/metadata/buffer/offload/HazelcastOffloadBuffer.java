package com.linkedin.metadata.buffer.offload;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicates;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.SizingPolicy;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Hazelcast-backed {@link OffloadBuffer}: one {@code IMap<K, V>} pending store, one {@code
 * IMap<String, String>} drain lock, and one {@code IMap<String, Long>} sequence counter — the shared
 * infra for every async offload (post-commit hooks now; retention in a follow-up). The drain-lock
 * (UUID-token {@code putIfAbsent} + TTL lease + token-fenced {@code remove}), {@link
 * PagingPredicate} FIFO drain, {@code removeIfSame} CAS clear, and {@code nextSequence} CAS loop
 * are lifted verbatim from the two prior implementations ({@code HazelcastPostCommitHookBuffer} and
 * {@code HazelcastCoalesceBuffer}), which were byte-for-byte identical on this surface.
 *
 * <p>A use supplies only the feature bits at construction: {@link MergePolicy}, {@link
 * SizingPolicy}, and a serializable drain {@link Comparator} (hooks order by the key's enqueue
 * sequence; retention orders by value-then-key). The shared {@code OffloadBufferFactory} wires the
 * maps + scheduling; the use's {@link DrainAction} + {@link OffloadContextResolver} supply the
 * replay logic and routing.
 *
 * <h2>Sizing × merge matrix</h2>
 *
 * <ul>
 *   <li>{@link SizingPolicy#REJECT_AT_CAP} + {@link MergePolicy#NO_COALESCE} — post-commit hooks:
 *       {@code enqueue} does a {@code size()} check then a plain {@code put}; at cap it returns
 *       {@code false} and the caller runs the work synchronously (no loss). No {@code EvictionConfig}.
 *   <li>{@link SizingPolicy#EVICT_LRU} + {@link MergePolicy#KEEP_MAX_LONG} — retention: {@code
 *       enqueue} coalesces via a serializable {@link KeepMaxLongProcessor} entry processor with a
 *       1s timeout; no {@code size()} check (the bound is the Hazelcast {@code EvictionConfig}).
 *       {@code V} must be {@link Long}.
 * </ul>
 *
 * <p>Other combinations are not meaningful today and will throw on the first relevant call.
 */
@Slf4j
public class HazelcastOffloadBuffer<K extends Serializable, V extends Serializable>
    implements OffloadBuffer<K, V> {

  private static final String SEQ_KEY = "v";
  // Fail-fast bound on the coalesce cluster op so a partitioned/GC-paused member cannot stall the
  // ingest thread for Hazelcast's (5-minute) default op timeout. On timeout the merge is dropped
  // (best-effort: under-coalescing = bloat, never data loss).
  private static final long MERGE_TIMEOUT_MS = 1000L;

  private final IMap<K, V> pendingMap;
  private final IMap<String, String> lockMap;
  private final IMap<String, Long> seqMap;
  private final int maxPendingEntries;
  private final MergePolicy mergePolicy;
  private final SizingPolicy sizingPolicy;
  private final Comparator<Map.Entry<K, V>> drainComparator;
  private final String metricPrefix;
  @Nullable private final MetricUtils metricUtils;

  public HazelcastOffloadBuffer(
      @Nonnull HazelcastInstance hazelcastInstance,
      @Nonnull String mapName,
      @Nonnull String lockMapName,
      @Nonnull String seqMapName,
      int maxPendingEntries,
      @Nonnull MergePolicy mergePolicy,
      @Nonnull SizingPolicy sizingPolicy,
      @Nonnull Comparator<Map.Entry<K, V>> drainComparator,
      @Nonnull String metricPrefix,
      @Nullable MetricUtils metricUtils) {
    this.pendingMap = hazelcastInstance.getMap(mapName);
    this.lockMap = hazelcastInstance.getMap(lockMapName);
    this.seqMap = hazelcastInstance.getMap(seqMapName);
    this.maxPendingEntries = Math.max(1, maxPendingEntries);
    this.mergePolicy = mergePolicy;
    this.sizingPolicy = sizingPolicy;
    this.drainComparator = drainComparator;
    this.metricPrefix = metricPrefix;
    this.metricUtils = metricUtils;
  }

  @Override
  public long nextSequence() {
    // Atomic increment via get + CAS (putIfAbsent for the first value, replace thereafter). Two
    // concurrent pods may both read the same `cur` and both lose the replace race; the loser
    // retries. This needs no CP subsystem (unlike IAtomicLong) and works on any IMap.
    for (; ; ) {
      Long cur = seqMap.get(SEQ_KEY);
      if (cur == null) {
        Long prev = seqMap.putIfAbsent(SEQ_KEY, 1L);
        if (prev == null) {
          return 1L;
        }
        cur = prev;
      }
      Long next = cur + 1;
      if (seqMap.replace(SEQ_KEY, cur, next)) {
        return next;
      }
      // CAS lost — retry with the latest value
    }
  }

  @Override
  public boolean enqueue(@Nonnull K key, @Nonnull V value) {
    try {
      if (sizingPolicy == SizingPolicy.REJECT_AT_CAP && pendingMap.size() >= maxPendingEntries) {
        log.warn(
            "{} buffer full ({} >= {}); rejecting enqueue so caller falls back to synchronous"
                + " execution (bounded memory, no data loss)",
            metricPrefix,
            pendingMap.size(),
            maxPendingEntries);
        increment(metricPrefix + "_buffer_full");
        return false;
      }
      if (mergePolicy == MergePolicy.NO_COALESCE) {
        // put, not merge: keys are unique (sequence), so this never overwrites a distinct entry.
        pendingMap.put(key, value);
      } else if (mergePolicy == MergePolicy.KEEP_MAX_LONG) {
        // Coalesce via a serializable entry processor shipped to the owning member. A plain lambda
        // merge cannot ship over the wire; only the built-in keep-max-long processor is supported.
        mergeKeepMaxLong(key, value);
      } else {
        throw new UnsupportedOperationException("Unsupported merge policy: " + mergePolicy);
      }
      increment(metricPrefix + "_enqueued");
      return true;
    } catch (Exception e) {
      // Do NOT swallow: return false so the caller falls back to synchronous execution. Losing the
      // async deferral is acceptable; losing the side effect is not.
      log.warn("{} buffer enqueue failed; caller will fall back to sync", metricPrefix, e);
      increment(metricPrefix + "_enqueue_failed");
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private void mergeKeepMaxLong(@Nonnull K key, @Nonnull V value) {
    if (!(value instanceof Long)) {
      throw new IllegalArgumentException(
          "KEEP_MAX_LONG merge policy requires Long values, got " + value.getClass());
    }
    long candidate = (Long) value;
    try {
      // KeepMaxLongProcessor is typed on Long; pendingMap is IMap<K,V>. Safe: we validated
      // value instanceof Long above, and KEEP_MAX_LONG is only wired for V=Long (retention).
      EntryProcessor<K, V, Void> processor =
          (EntryProcessor<K, V, Void>) (EntryProcessor<?, ?, ?>) new KeepMaxLongProcessor<>(candidate);
      pendingMap
          .submitToKey(key, processor)
          .toCompletableFuture()
          .get(MERGE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      increment(metricPrefix + "_merge_timeout");
    } catch (ExecutionException e) {
      increment(metricPrefix + "_merge_failed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      increment(metricPrefix + "_merge_interrupted");
    }
  }

  @Override
  public boolean defersApply() {
    return true;
  }

  @Override
  @Nonnull
  public List<Map.Entry<K, V>> drain(int limit) {
    if (limit <= 0) {
      return List.of();
    }
    // Explicit comparator so paging does NOT fall back to natural ordering, which would cast the
    // key to Comparable and throw ClassCastException for non-Comparable keys. The comparator is
    // use-supplied (hooks: enqueue sequence; retention: value-then-key) and must be Serializable.
    PagingPredicate<K, V> page = Predicates.pagingPredicate(drainComparator, limit);
    return new ArrayList<>(pendingMap.entrySet(page));
  }

  @Override
  public boolean removeIfSame(@Nonnull K key, @Nonnull V expected) {
    return pendingMap.remove(key, expected);
  }

  @Override
  public void requeue(@Nonnull K key, @Nonnull V value) {
    // Same unique key → value update, not a new entry. Keeps the entry's original FIFO position.
    pendingMap.put(key, value);
  }

  @Override
  public int size() {
    return pendingMap.size();
  }

  @Override
  @Nullable
  public Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
    // Non-reentrant: IMap.tryLock is re-entrant for the same thread; putIfAbsent fails if the key is
    // present even for this thread. The stored value is a per-acquire fencing token so release only
    // clears our own lock. TTL = lease so a crashed drainer does not wedge the lock forever.
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
          "{} drain lock '{}' not released by owner — lease likely expired and it was re-acquired",
          metricPrefix,
          lockName);
    }
  }

  private void increment(@Nonnull String metric) {
    if (metricUtils != null) {
      metricUtils.increment(HazelcastOffloadBuffer.class, metric, 1);
    }
  }

  /** Keep-max coalescing {@link EntryProcessor} for a single key (V = Long). */
  static final class KeepMaxLongProcessor<K extends Serializable>
      implements EntryProcessor<K, Long, Void> {
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
