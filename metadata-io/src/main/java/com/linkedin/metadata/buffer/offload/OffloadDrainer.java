package com.linkedin.metadata.buffer.offload;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Generic background drainer over an {@link OffloadBuffer}. This is the shared infra half of an
 * async offload: a cluster-wide single-winner drain lock, a bounded {@link OffloadBuffer#drain}
 * batch, grouping by {@link OffloadContextResolver#groupKey} (e.g. per route), per-group {@link
 * OperationContext} reconstruction via {@link OffloadContextResolver#resolveOpContext}, and a
 * lease-exceeded guard. The use-specific replay is delegated to a {@link DrainAction}.
 *
 * <p><b>Why the {@link DrainAction} owns entry removal.</b> A use may process a group partially
 * (e.g. hooks isolate a poison MCL and still replay the rest). For that to be safe the action must
 * {@link OffloadBuffer#removeIfSame} each entry it finishes and {@link OffloadBuffer#requeue} any
 * it wants to retry — so the action receives the buffer. The drainer itself only removes entries on
 * a <em>permanent</em> resolve failure ({@link UnresolvableOffloadKeyException}); on a transient
 * resolver failure it either leaves the un-removed entries for the next tick (at-least-once, when
 * {@code backoffEnabled=false}) or moves them to a backoff limbo (when {@code backoffEnabled=true},
 * re-merged after {@code backoffTicks} ticks — see below). A transient {@link DrainAction} failure
 * always leaves entries for next-tick retry (at-least-once), matching the use's own apply-failure
 * semantics.
 *
 * <p><b>Transient-failure backoff (optional).</b> {@link OffloadBuffer#drain} is non-destructive: a
 * {@code PagingPredicate} restarts each call and returns the same first page until {@link
 * OffloadBuffer#removeIfSame} clears it. So a key whose {@link OffloadContextResolver#groupKey} or
 * {@link OffloadContextResolver#resolveOpContext} throws a <em>transient</em> {@link
 * RuntimeException} (e.g. a routing-lookup blip) would occupy the first page every tick and starve
 * every key behind it. When {@code backoffEnabled=true} the drainer, on a transient resolver
 * failure, removes the key from the buffer and re-merges it (via {@link OffloadBuffer#enqueue},
 * which applies the use's merge policy) after {@code backoffTicks} ticks. The failing key is out of
 * the buffer during the backoff window so {@code drain} surfaces the keys behind it and they
 * progress; a transient blip self-heals and a persistent failure just retries at a slower rate
 * rather than wedging the drainer. Backoff applies ONLY to resolver failures (routing), not to
 * {@link DrainAction} failures (apply). Default off: hooks never throw transient resolver failures
 * ({@code SimpleHookContextResolver.groupKey} is constant), so backoff is dead code for them;
 * retention enables it when its resolver can fail transiently.
 *
 * <p><b>Scheduling.</b> {@link #tick()} carries no {@code @Scheduled} annotation; the shared {@code
 * OffloadBufferFactory} registers it with a Spring {@code TaskScheduler} at the use-specific {@code
 * drainIntervalMs}. This removes the per-use {@code @EnableScheduling} config — one fewer "infra
 * key" per reuse.
 *
 * @param <K> buffer key type
 * @param <V> buffer payload type
 */
@Slf4j
public class OffloadDrainer<K extends Serializable, V extends Serializable> {

  private static final String DRAIN_LOCK_NAME = "drain";

  private final OffloadBuffer<K, V> buffer;
  private final OffloadContextResolver<K> contextResolver;
  private final OperationContext systemOperationContext;
  private final DrainAction<K, V> drainAction;
  private final int batchSize;
  private final Duration drainLockLease;
  private final boolean enabled;
  private final String metricPrefix;
  @Nullable private final MetricUtils metricUtils;

  private final boolean backoffEnabled;
  private final long backoffTicks;
  // currentTick and the backoff maps are read/written only inside drainBatch(), which is itself
  // guarded by the cluster-wide drain lock acquired in tick(). ConcurrentHashMap defends against a
  // non-scheduler caller (test, admin endpoint) invoking tick() concurrently with the scheduler —
  // a HashMap would corrupt on concurrent put. The single-tick-at-a-time invariant is still
  // required for correctness (two concurrent ticks on the same pod would race the drain lock and
  // one would no-op, but the maps must not corrupt even if that guard is ever relaxed).
  //
  // <p>Bounded in practice: a key only enters these maps on a transient resolver failure, and the
  // only keys eligible in a given tick are those in the current drain batch (size = batchSize, e.g.
  // 500 for retention). A key is removed once its backoff expires (re-merged) or the resolver later
  // throws UnresolvableOffloadKeyException (permanent drop, handled on the next drain of the
  // re-merged key). So the maps never hold more than the drain batch size — they are bounded by
  // batchSize, not unbounded. No explicit cap is needed because the inflow (drain batch) is the
  // only source and it is itself capped.
  private long currentTick = 0L;
  private final ConcurrentHashMap<K, Long> transientRetryAt = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<K, V> transientRetryValue = new ConcurrentHashMap<>();

  public OffloadDrainer(
      @Nonnull OffloadBuffer<K, V> buffer,
      @Nonnull OffloadContextResolver<K> contextResolver,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull DrainAction<K, V> drainAction,
      int batchSize,
      long drainLockLeaseMs,
      boolean enabled,
      @Nonnull String metricPrefix,
      @Nullable MetricUtils metricUtils) {
    this(
        buffer,
        contextResolver,
        systemOperationContext,
        drainAction,
        batchSize,
        drainLockLeaseMs,
        enabled,
        metricPrefix,
        metricUtils,
        false,
        5L);
  }

  /**
   * Full constructor with transient-backoff control.
   *
   * @param backoffEnabled when true, transient resolver failures move the key to a backoff limbo
   *     (removed + re-merged after {@code backoffTicks}) to avoid first-page starvation; when
   *     false, transient failures leave the key in-buffer for next-tick retry.
   * @param backoffTicks backoff window in ticks (used only when {@code backoffEnabled}); must be
   *     {@code >= 1}.
   */
  public OffloadDrainer(
      @Nonnull OffloadBuffer<K, V> buffer,
      @Nonnull OffloadContextResolver<K> contextResolver,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull DrainAction<K, V> drainAction,
      int batchSize,
      long drainLockLeaseMs,
      boolean enabled,
      @Nonnull String metricPrefix,
      @Nullable MetricUtils metricUtils,
      boolean backoffEnabled,
      long backoffTicks) {
    this.buffer = buffer;
    this.contextResolver = contextResolver;
    this.systemOperationContext = systemOperationContext;
    this.drainAction = drainAction;
    this.batchSize = batchSize;
    this.drainLockLease = Duration.ofMillis(drainLockLeaseMs);
    this.enabled = enabled;
    this.metricPrefix = metricPrefix;
    this.metricUtils = metricUtils;
    this.backoffEnabled = backoffEnabled;
    this.backoffTicks = Math.max(1L, backoffTicks);
  }

  /**
   * One drain tick. Idempotent under concurrent ticks (cluster-wide drain lock). Must NOT be called
   * concurrently on the same drainer instance — {@code scheduleWithFixedDelay} guarantees
   * sequential ticks from one scheduler, but an external caller (test, admin endpoint) invoking
   * {@code tick()} while the scheduler is running would race the drain lock. The backoff maps use
   * {@link ConcurrentHashMap} so a race corrupts nothing (one tick no-ops on the lock), but the
   * single-tick invariant is still required for drain correctness.
   */
  public void tick() {
    if (!enabled) {
      return;
    }
    Object lockToken = buffer.tryAcquireDrainLock(DRAIN_LOCK_NAME, drainLockLease);
    if (lockToken == null) {
      return;
    }
    try {
      long startMs = System.currentTimeMillis();
      drainBatch();
      long elapsedMs = System.currentTimeMillis() - startMs;
      if (elapsedMs > drainLockLease.toMillis()) {
        log.warn(
            "{} drain took {}ms, exceeding the {}ms lock lease; another pod may drain concurrently"
                + " (safe but wasteful — replays should be idempotent). Raise drainLockLeaseMs or"
                + " lower drainBatchSize.",
            metricPrefix,
            elapsedMs,
            drainLockLease.toMillis());
        increment(metricPrefix + "_drain_exceeded_lease");
      }
    } finally {
      buffer.releaseDrainLock(DRAIN_LOCK_NAME, lockToken);
    }
  }

  private void drainBatch() {
    // currentTick is a monotonic per-tick counter used only for transient-backoff scheduling
    // (transientRetryAt stores absolute tick deadlines). It is incremented unconditionally even
    // when the batch is empty so backoff deadlines stay consistent across idle ticks. NOTE:
    // `enabled` is final (set at construction from the feature flag) — currentTick is NOT
    // runtime-togglable and the scheduler is not dynamically re-registered; toggling the flag
    // requires a bean restart. Do not add a runtime toggle without reworking the scheduler
    // lifecycle.
    currentTick++;
    // Re-merge keys whose transient backoff has expired so they retry on this tick. They were
    // removed from the buffer when the transient failure was caught, so drain() now surfaces them
    // again alongside any other queued keys. requeue() applies the use's merge policy SYNCHRONOUSLY
    // (keep-max for retention → coalesces with any newer version enqueued during backoff; put for
    // hooks, but hooks run with backoff off). Synchronous (not enqueue's fire-and-forget) is
    // required here: the re-merged entry must be visible to the drain() on THIS tick, else the
    // backoff window's final tick would drain empty and the key would never be re-applied. The
    // drainer is a background thread, so blocking on the merge is correct and affordable (this is
    // NOT the ingest hot path that enqueue's fire-and-forget was designed to protect).
    if (backoffEnabled && !transientRetryAt.isEmpty()) {
      Iterator<Map.Entry<K, Long>> it = transientRetryAt.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<K, Long> e = it.next();
        if (e.getValue() <= currentTick) {
          V value = transientRetryValue.remove(e.getKey());
          it.remove();
          if (value != null) {
            buffer.requeue(e.getKey(), value);
          }
        }
      }
    }

    List<Map.Entry<K, V>> batch = buffer.drain(batchSize);
    if (batch.isEmpty()) {
      return;
    }
    Map<String, List<Map.Entry<K, V>>> groups = new LinkedHashMap<>();
    // Permanent (unresolvable) drops are collected for a single batch removeAll — these keys are
    // poison (malformed/routing) and ingest never re-merges a malformed key, so non-CAS is safe.
    List<Map.Entry<K, V>> unresolvableGroupKey = new ArrayList<>();
    // Backoff drops are NOT batched: backoff is retention-only (hooks run with backoff off) and
    // retention keys are reused by concurrent ingest, so a non-CAS removeAll would clobber a
    // re-merged higher version (and the stale backoff value would later overwrite it). Per-entry
    // removeIfSame CAS guards exactly this — the entry survives if a higher version re-merged.
    for (Map.Entry<K, V> entry : batch) {
      try {
        String g = contextResolver.groupKey(entry.getKey());
        groups.computeIfAbsent(g, k -> new ArrayList<>()).add(entry);
      } catch (UnresolvableOffloadKeyException e) {
        log.warn(
            "Dropping unresolvable {} key {}; {}", metricPrefix, entry.getKey(), e.getMessage());
        unresolvableGroupKey.add(entry);
        increment(metricPrefix + "_unresolvable_key");
      } catch (RuntimeException e) {
        if (backoffEnabled) {
          // Move to backoff limbo so drain() surfaces the keys behind this one; re-merge after
          // backoffTicks. Without this, a persistently-failing key starves the first page (drain is
          // non-destructive and returns the same first page until removeIfSame clears it). CAS
          // remove: if a higher version re-merged in the meantime, the entry survives and is
          // re-drained next tick (the backoff re-merge later coalesces its stale value away).
          buffer.removeIfSame(entry.getKey(), entry.getValue());
          transientRetryAt.put(entry.getKey(), currentTick + backoffTicks);
          transientRetryValue.put(entry.getKey(), entry.getValue());
          log.warn(
              "Transient {} groupKey failure for {}; backing off for {} ticks",
              metricPrefix,
              entry.getKey(),
              backoffTicks,
              e);
        } else {
          log.warn(
              "Transient {} groupKey failure for {}; leaving for retry",
              metricPrefix,
              entry.getKey(),
              e);
        }
      }
    }
    // Batch-remove only the permanent (unresolvable) drops: no re-merge of poison keys.
    if (!unresolvableGroupKey.isEmpty()) {
      buffer.removeAll(unresolvableGroupKey);
    }
    for (List<Map.Entry<K, V>> group : groups.values()) {
      replayGroup(group);
    }
    if (batch.size() == batchSize) {
      log.info(
          "{} drain hit the batch cap ({}); buffer may be filling faster than it drains",
          metricPrefix,
          batchSize);
    }
  }

  private void replayGroup(@Nonnull List<Map.Entry<K, V>> entries) {
    K firstKey = entries.get(0).getKey();
    OperationContext opContext;
    try {
      opContext = contextResolver.resolveOpContext(firstKey, systemOperationContext);
    } catch (UnresolvableOffloadKeyException e) {
      log.warn(
          "Dropping {} unresolvable {} entries; {}", entries.size(), metricPrefix, e.getMessage());
      buffer.removeAll(entries);
      increment(metricPrefix + "_unresolvable_key", entries.size());
      return;
    } catch (RuntimeException e) {
      if (backoffEnabled) {
        // Same backoff as the groupKey path: move the whole group to the limbo so drain() surfaces
        // the keys behind them, re-merge after backoffTicks. Per-entry removeIfSame CAS (not
        // removeAll): backoff is retention-only and retention keys are reused by concurrent
        // ingest, so a non-CAS batch remove would clobber a re-merged higher version and the
        // stale backoff value would later overwrite it. CAS leaves the entry in place if a
        // higher version re-merged, so it is re-drained next tick.
        for (Map.Entry<K, V> entry : entries) {
          buffer.removeIfSame(entry.getKey(), entry.getValue());
          transientRetryAt.put(entry.getKey(), currentTick + backoffTicks);
          transientRetryValue.put(entry.getKey(), entry.getValue());
        }
        log.warn(
            "Transient {} resolveOpContext failure for {}; backing off {} entries for {} ticks",
            metricPrefix,
            firstKey,
            entries.size(),
            backoffTicks,
            e);
      } else {
        log.warn(
            "Transient {} resolveOpContext failure for {}; leaving {} entries for retry",
            metricPrefix,
            firstKey,
            entries.size(),
            e);
      }
      return;
    }
    try {
      drainAction.apply(entries, opContext, buffer);
    } catch (UnresolvableOffloadKeyException e) {
      // Action signaled a permanent failure for the whole group; drop to avoid re-throwing every
      // tick. (Fine-grained per-entry permanent drops are the action's job via removeIfSame.)
      log.warn(
          "Dropping {} {} entries after permanent action failure; {}",
          entries.size(),
          metricPrefix,
          e.getMessage());
      buffer.removeAll(entries);
      increment(metricPrefix + "_unresolvable_key", entries.size());
    } catch (Throwable t) {
      // Transient: leave un-removed entries for the next tick (at-least-once). The action is
      // expected to have removed any entries it finished before throwing.
      log.warn(
          "{} action failed for {} entries; leaving un-removed for retry (at-least-once)",
          metricPrefix,
          entries.size(),
          t);
      increment(metricPrefix + "_action_failed", entries.size());
    }
  }

  private void increment(@Nonnull String metric) {
    increment(metric, 1);
  }

  private void increment(@Nonnull String metric, long n) {
    if (metricUtils != null) {
      metricUtils.increment(OffloadDrainer.class, metric, n);
    }
  }
}
