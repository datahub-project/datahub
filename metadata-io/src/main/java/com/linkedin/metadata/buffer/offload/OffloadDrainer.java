package com.linkedin.metadata.buffer.offload;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Generic background drainer over an {@link OffloadBuffer}. This is the shared infra half of an
 * async offload: a cluster-wide single-winner drain lock, a bounded {@link OffloadBuffer#drain}
 * batch, grouping by {@link OffloadContextResolver#groupKey} (e.g. per tenant), per-group {@link
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
 * RuntimeException} (e.g. a tenant-lookup service blip) would occupy the first page every tick and
 * starve every key behind it. When {@code backoffEnabled=true} the drainer, on a transient resolver
 * failure, removes the key from the buffer and re-merges it (via {@link OffloadBuffer#enqueue},
 * which applies the use's merge policy) after {@code backoffTicks} ticks. The failing key is out of
 * the buffer during the backoff window so {@code drain} surfaces the keys behind it and they
 * progress; a transient blip self-heals and a persistent failure just retries at a slower rate
 * rather than wedging the drainer. Backoff applies ONLY to resolver failures (routing), not to
 * {@link DrainAction} failures (apply). Default off: hooks never throw transient resolver failures
 * ({@code SimpleHookContextResolver.groupKey} is constant), so backoff is dead code for them;
 * retention (cloud, tenant-aware) enables it.
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
  private long currentTick = 0L;
  private final Map<K, Long> transientRetryAt = new HashMap<>();
  private final Map<K, V> transientRetryValue = new HashMap<>();

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

  /** One drain tick. Idempotent under concurrent ticks (cluster-wide drain lock). */
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
    currentTick++;
    // Re-merge keys whose transient backoff has expired so they retry on this tick. They were
    // removed from the buffer when the transient failure was caught, so drain() now surfaces them
    // again alongside any other queued keys. enqueue() applies the use's merge policy (keep-max
    // for retention → coalesces with any newer version enqueued during backoff; put for hooks,
    // but hooks run with backoff off).
    if (backoffEnabled && !transientRetryAt.isEmpty()) {
      Iterator<Map.Entry<K, Long>> it = transientRetryAt.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<K, Long> e = it.next();
        if (e.getValue() <= currentTick) {
          V value = transientRetryValue.remove(e.getKey());
          it.remove();
          if (value != null) {
            buffer.enqueue(e.getKey(), value);
          }
        }
      }
    }

    List<Map.Entry<K, V>> batch = buffer.drain(batchSize);
    if (batch.isEmpty()) {
      return;
    }
    Map<String, List<Map.Entry<K, V>>> groups = new LinkedHashMap<>();
    for (Map.Entry<K, V> entry : batch) {
      try {
        String g = contextResolver.groupKey(entry.getKey());
        groups.computeIfAbsent(g, k -> new ArrayList<>()).add(entry);
      } catch (UnresolvableOffloadKeyException e) {
        log.warn(
            "Dropping unresolvable {} key {}; {}", metricPrefix, entry.getKey(), e.getMessage());
        buffer.removeIfSame(entry.getKey(), entry.getValue());
        increment(metricPrefix + "_unresolvable_key");
      } catch (RuntimeException e) {
        if (backoffEnabled) {
          // Move to backoff limbo so drain() surfaces the keys behind this one; re-merge after
          // backoffTicks. Without this, a persistently-failing key starves the first page (drain is
          // non-destructive and returns the same first page until removeIfSame clears it).
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
      for (Map.Entry<K, V> entry : entries) {
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
      increment(metricPrefix + "_unresolvable_key", entries.size());
      return;
    } catch (RuntimeException e) {
      if (backoffEnabled) {
        // Same backoff as the groupKey path: move the whole group to the limbo so drain() surfaces
        // the keys behind them, re-merge after backoffTicks.
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
      for (Map.Entry<K, V> entry : entries) {
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
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
