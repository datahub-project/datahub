package com.linkedin.metadata.buffer.offload;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.SizingPolicy;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Hazelcast-backed {@link OffloadBuffer}: one {@code IMap<K, V>} pending store, one {@code
 * IMap<String, String>} drain lock, and one {@code IMap<String, Long>} sequence counter — the
 * shared infra for every async offload (post-commit hooks now; retention in a follow-up). The
 * drain-lock (UUID-token {@code putIfAbsent} + TTL lease + token-fenced {@code remove}), {@link
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
 *       {@code enqueue} checks a local per-JVM {@code approxSize} counter (NOT a cluster-wide
 *       {@code IMap.size()} round-trip, which would block the ingest hot path) then a plain
 *       {@code put}; at cap it returns {@code false} and the caller runs the work synchronously
 *       (no loss). The cap is enforced per-pod, so the cluster-wide pending total is approximately
 *       {@code ingestingPods × maxPendingEntries} — see {@link
 *       com.linkedin.metadata.config.offload.OffloadBufferProperties#getMaxPendingEntries()}.
 *       No {@code EvictionConfig}.
 *   <li>{@link SizingPolicy#EVICT_LRU} + {@link MergePolicy#KEEP_MAX_LONG} — retention: {@code
 *       enqueue} coalesces via a serializable {@link KeepMaxLongProcessor} entry processor
 *       <b>fire-and-forget</b> (no {@code .get()} on the ingest thread); no {@code size()} check
 *       (the bound is the Hazelcast {@code EvictionConfig}). {@code V} must be {@link Long}.
 * </ul>
 *
 * <p>Other combinations are not meaningful today and will throw on the first relevant call.
 */
@Slf4j
public class HazelcastOffloadBuffer<K extends Serializable, V extends Serializable>
    implements OffloadBuffer<K, V> {

  private final IMap<K, V> pendingMap;
  private final IMap<String, String> lockMap;
  // IMap-backed sequence counter. nextSequence() runs a single atomic EntryProcessor increment on
  // the owning partition — one round-trip, no get+replace CAS spin loop (which retries under
  // cross-pod contention and round-trips on every failed CAS). Works on any Hazelcast shape,
  // including a single-member embedded test node (no CP-subsystem/Raft quorum required, unlike
  // IAtomicLong which needs a 3+ member CP group).
  private final IMap<String, Long> seqMap;
  private final int maxPendingEntries;
  private final MergePolicy mergePolicy;
  private final SizingPolicy sizingPolicy;
  private final Comparator<Map.Entry<K, V>> drainComparator;
  private final String metricPrefix;
  @Nullable private final MetricUtils metricUtils;
  // Local soft estimate of pendingMap size for the REJECT_AT_CAP path. IMap.size() is a
  // cluster-wide round-trip (contacts every partition owner); under high ingest load that is worse
  // than the inline hook execution we are trying to avoid. This counter increments on a successful
  // put and decrements on a successful removeIfSame, so it tracks actual entries approximately
  // (drain() is non-destructive and does not touch it; requeue overwrites an existing key and does
  // not change the count). It undershoots after a full restart (starts at 0 while the map may have
  // in-flight entries) and converges as entries drain — acceptable for a bounded-buffer contract
  // where approximate rejection (admit a few over the cap, or reject a few under) is safe.
  private final AtomicInteger approxSize = new AtomicInteger(0);

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
    // One atomic EntryProcessor increment on the owning partition — one round-trip, no
    // get+replace CAS spin loop (which retries under cross-pod contention and round-trips on every
    // failed CAS). Returns a globally-unique monotonic long; the drain comparator uses it for FIFO
    // ordering across pods.
    return incrementSeq(1);
  }

  @Override
  public long nextSequence(int count) {
    // One atomic EntryProcessor add for the whole batch — one round-trip for N MCLs, not N.
    // The caller consumes [highest - count + 1 .. highest] in order; FIFO within the batch is
    // preserved because the caller assigns the block sequentially to its in-order MCL list.
    if (count <= 0) {
      return 0L;
    }
    return incrementSeq(count);
  }

  private long incrementSeq(int delta) {
    Long next = (Long) seqMap.executeOnKey(SEQ_KEY, new SeqIncrementProcessor(delta));
    return next;
  }

  private static final String SEQ_KEY = "seq";

  @Override
  public boolean enqueue(@Nonnull K key, @Nonnull V value) {
    try {
      if (sizingPolicy == SizingPolicy.REJECT_AT_CAP && approxSize.get() >= maxPendingEntries) {
        log.warn(
            "{} buffer full (approx {} >= {}); rejecting enqueue so caller falls back to"
                + " synchronous execution (bounded memory, no data loss)",
            metricPrefix,
            approxSize.get(),
            maxPendingEntries);
        increment(metricPrefix + "_buffer_full");
        return false;
      }
      if (mergePolicy == MergePolicy.NO_COALESCE) {
        // put, not merge: keys are unique (sequence), so this never overwrites a distinct entry.
        pendingMap.put(key, value);
        approxSize.incrementAndGet();
      } else if (mergePolicy == MergePolicy.KEEP_MAX_LONG) {
        // Coalesce via a serializable entry processor shipped to the owning member. A plain lambda
        // merge cannot ship over the wire; only the built-in keep-max-long processor is supported.
        mergeKeepMaxLong(key, value);
      } else {
        throw new UnsupportedOperationException("Unsupported merge policy: " + mergePolicy);
      }
      increment(metricPrefix + "_enqueued");
      return true;
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Programming / wiring errors (e.g. KEEP_MAX_LONG with a non-Long value) must propagate —
      // returning false would hide a misconfiguration as a soft sync-fallback.
      throw e;
    } catch (Exception e) {
      // Transient infra failure: return false so the caller falls back to synchronous execution.
      // Losing the async deferral is acceptable; losing the side effect is not.
      log.warn("{} buffer enqueue failed; caller will fall back to sync", metricPrefix, e);
      increment(metricPrefix + "_enqueue_failed");
      return false;
    }
  }

  @Override
  public boolean enqueueBatch(@Nonnull List<Map.Entry<K, V>> entries) {
    if (entries.isEmpty()) {
      return true;
    }
    try {
      if (sizingPolicy == SizingPolicy.REJECT_AT_CAP
          && approxSize.get() + entries.size() > maxPendingEntries) {
        // All-or-nothing at the cap: reject the whole batch so the caller runs every entry
        // synchronously (no data loss). Coarser than per-item admit, but one round-trip for the
        // common (not-full) case is the point — buffer-full is the designed sync backpressure path.
        log.warn(
            "{} buffer full (approx {} + batch {} > {}); rejecting batch so caller falls back to"
                + " synchronous execution (bounded memory, no data loss)",
            metricPrefix,
            approxSize.get(),
            entries.size(),
            maxPendingEntries);
        increment(metricPrefix + "_buffer_full");
        return false;
      }
      if (mergePolicy == MergePolicy.NO_COALESCE) {
        // One pipelined round-trip to all partition owners (IMap.putAll), not N serial puts. Keys
        // are unique (sequence), so no overwrite of a distinct entry.
        Map<K, V> batch = new HashMap<>(entries.size());
        for (Map.Entry<K, V> e : entries) {
          batch.put(e.getKey(), e.getValue());
        }
        try {
          pendingMap.putAll(batch);
        } catch (Exception putAllFailure) {
          // putAll is NOT atomic — some entries may already be written before it threw. If we
          // return false here without cleanup, the caller runs every entry synchronously AND
          // the partially-written entries stay in the buffer for async replay → double work.
          // Remove the whole batch (best-effort) so the sync fallback is the only execution.
          // Safe because NO_COALESCE keys are unique per-sequence and never reused by ingest, so
          // removeAll cannot clobber a concurrent re-merge (unlike retention's reused keys). If
          // removeAll itself fails, the partial entries stay and are async-replayed — at-least-once,
          // idempotent hooks, so correct but redundant.
          log.warn(
              "{} buffer batch putAll failed; cleaning partial writes before sync fallback",
              metricPrefix,
              putAllFailure);
          try {
            removeAll(entries);
          } catch (Exception cleanupFailure) {
            log.warn(
                "{} partial-write cleanup failed; entries will be async-replayed (at-least-once)",
                metricPrefix,
                cleanupFailure);
          }
          increment(metricPrefix + "_enqueue_failed");
          return false;
        }
        approxSize.addAndGet(entries.size());
      } else if (mergePolicy == MergePolicy.KEEP_MAX_LONG) {
        // Retention: fire-and-forget per key (keep-max is commutative + associative, so async
        // completion order is irrelevant). submitToKey is non-blocking, so this is N async
        // submissions, not N blocking round-trips.
        for (Map.Entry<K, V> e : entries) {
          mergeKeepMaxLong(e.getKey(), e.getValue());
        }
      } else {
        throw new UnsupportedOperationException("Unsupported merge policy: " + mergePolicy);
      }
      increment(metricPrefix + "_enqueued", entries.size());
      return true;
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      log.warn("{} buffer batch enqueue failed; caller will fall back to sync", metricPrefix, e);
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
    // KeepMaxLongProcessor is typed on Long; pendingMap is IMap<K,V>. Safe: we validated
    // value instanceof Long above, and KEEP_MAX_LONG is only wired for V=Long (retention).
    EntryProcessor<K, V, Void> processor =
        (EntryProcessor<K, V, Void>)
            (EntryProcessor<?, ?, ?>) new KeepMaxLongProcessor<>(candidate);
    // Fire-and-forget: keep-max is idempotent and convergent (a later enqueue re-offers the
    // same-or-higher version), so we do NOT block the ingest thread on .get() waiting for the
    // owning member to confirm. The prior .get(MERGE_TIMEOUT_MS) blocked ingest up to 1s per
    // entry — unacceptable on the hot path (async deferral exists to speed ingest, not slow it).
    // A processor failure is observed async via whenComplete (metric + log); a lost submit
    // (member unreachable) throws synchronously and is caught by enqueue's catch(Exception)
    // → return false → caller falls back to sync. Self-healing: the next enqueue re-offers.
    pendingMap
        .submitToKey(key, processor)
        .toCompletableFuture()
        .whenComplete(
            (v, t) -> {
              if (t != null) {
                log.warn(
                    "{} keep-max merge failed async for {}; convergent on next enqueue",
                    metricPrefix,
                    key,
                    t);
                increment(metricPrefix + "_merge_failed");
              }
            });
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
    boolean removed = pendingMap.remove(key, expected);
    if (removed) {
      approxSize.decrementAndGet();
    }
    return removed;
  }

  @Override
  public void removeAll(@Nonnull List<Map.Entry<K, V>> entries) {
    if (entries.isEmpty()) {
      return;
    }
    // One pipelined round-trip to all partition owners (IMap.removeAll(Predicate)), not N serial
    // CAS removes. IMap has no removeAll(Set<K>) overload — only removeAll(Predicate<K,V>) — so
    // we ship a serializable key-set predicate that each partition evaluates against its local
    // entries. Non-CAS: removes the keys unconditionally. Safe ONLY when the caller guarantees no
    // requeue of these keys between drain and here (see OffloadBuffer#removeAll contract). The
    // retry path (remove-then-requeue on the same key) MUST use removeIfSame, not this.
    java.util.Set<K> keys = new java.util.HashSet<>(entries.size());
    for (Map.Entry<K, V> e : entries) {
      keys.add(e.getKey());
    }
    pendingMap.removeAll(new KeySetPredicate<>(keys));
    // Soft-estimate adjustment: floor at 0 so a stale over-decrement (keys already drained by a
    // prior tick) cannot drive the counter negative.
    approxSize.updateAndGet(s -> Math.max(0, s - keys.size()));
  }

  @Override
  public void requeue(@Nonnull K key, @Nonnull V value) {
    // Synchronous re-insert applying the use's merge policy. Used by the drainer's transient-backoff
    // re-merge and by DrainAction retry paths — both run on the background drainer thread (NOT the
    // ingest hot path), so blocking until the entry is visible is correct and affordable. This
    // deliberately differs from enqueue(), which is fire-and-forget on the ingest path (must not
    // block ingest >100ms).
    //   NO_COALESCE (hooks): plain put — keys are unique; retry re-inserts the same key with an
    // updated value (e.g. bumped retry count), keeping the original FIFO position.
    //   KEEP_MAX_LONG (retention): SYNCHRONOUS keep-max merge via executeOnKey (blocks until the
    // owning member confirms), so the re-merged entry is visible to the same-tick drain() that
    // follows the re-merge in OffloadDrainer.drainBatch. A fire-and-forget submitToKey here would
    // race drain() — the re-merged entry might not be visible yet, so the backoff window's final
    // tick would drain empty and the key would never be re-applied. Coalesces with any newer version
    // enqueued during backoff (keep-max), so a stale backoff value never clobbers a higher one.
    if (mergePolicy == MergePolicy.KEEP_MAX_LONG) {
      mergeKeepMaxLongSync(key, value);
    } else {
      pendingMap.put(key, value);
    }
  }

  @SuppressWarnings("unchecked")
  private void mergeKeepMaxLongSync(@Nonnull K key, @Nonnull V value) {
    if (!(value instanceof Long)) {
      throw new IllegalArgumentException(
          "KEEP_MAX_LONG merge policy requires Long values, got " + value.getClass());
    }
    long candidate = (Long) value;
    EntryProcessor<K, V, Void> processor =
        (EntryProcessor<K, V, Void>)
            (EntryProcessor<?, ?, ?>) new KeepMaxLongProcessor<>(candidate);
    // Synchronous (blocks until the owning member confirms the merge). Drainer thread only —
    // never the ingest hot path. See requeue() javadoc for why visibility before drain() matters.
    pendingMap.executeOnKey(key, processor);
  }

  @Override
  public int size() {
    return pendingMap.size();
  }

  @Override
  @Nullable
  public Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
    // Non-reentrant: IMap.tryLock is re-entrant for the same thread; putIfAbsent fails if the key
    // is
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
    increment(metric, 1);
  }

  private void increment(@Nonnull String metric, long n) {
    if (metricUtils != null) {
      metricUtils.increment(HazelcastOffloadBuffer.class, metric, n);
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

  /**
   * Atomic add-and-return {@link EntryProcessor} for the sequence counter map (key = {@link
   * #SEQ_KEY}). Runs on the owning partition so the read-modify-write is server-side and
   * contention-free across pods — no client-side get+replace CAS spin.
   */
  static final class SeqIncrementProcessor implements EntryProcessor<String, Long, Long> {
    private static final long serialVersionUID = 1L;

    private final long delta;

    SeqIncrementProcessor(long delta) {
      this.delta = delta;
    }

    @Override
    public Long process(Map.Entry<String, Long> entry) {
      Long current = entry.getValue();
      long next = (current == null ? 0L : current) + delta;
      entry.setValue(next);
      return next;
    }
  }

  /**
   * Serializable key-set {@link Predicate} for batch removal via {@code IMap.removeAll(Predicate)}.
   * IMap exposes no {@code removeAll(Set<K>)} overload; this predicate ships to each partition,
   * which removes its locally-owned matching keys in one round-trip.
   */
  static final class KeySetPredicate<K extends Serializable, V extends Serializable>
      implements Predicate<K, V> {
    private static final long serialVersionUID = 1L;

    private final java.util.Set<K> keys;

    KeySetPredicate(java.util.Set<K> keys) {
      this.keys = keys;
    }

    @Override
    public boolean apply(Map.Entry<K, V> entry) {
      return entry != null && keys.contains(entry.getKey());
    }
  }
}
