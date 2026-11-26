package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffers;
import com.linkedin.metadata.config.retention.RetentionBufferProperties;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.retention.RetentionBatchEntry;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.UnresolvableRetentionKeyException;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Background drainer over a {@link CoalesceBuffer} of pending retention keys. All pods share one
 * cluster-wide drain lock (Hazelcast), so exactly one pod applies retention per tick and the rest
 * no-op. The drained batch is grouped by {@link RetentionContextResolver#groupKey} so entries that
 * share a routing context are applied in one {@link
 * RetentionService#applyRetentionBatchWithPolicyDefaults} call with a single reconstructed {@link
 * OperationContext} (from {@link RetentionContextResolver#resolveOpContext}); each (urn, aspect)
 * pair still runs in its own transaction so a poison pair fails and retries on its own without
 * blocking the rest of the group. Keys returned as committed are cleared via {@code removeIfSame};
 * everything else stays for the next tick to retry (no retention_dlq table in v1 — see plan Global
 * Constraints).
 *
 * <p>Original drained keys are retained for the {@code removeIfSame} clear step — the service
 * echoes back the committed {@link RetentionKey}s (original instances, not reconstructed ones), so
 * cross-off is by {@link RetentionKey} equals (explicit per subtype). A key subtype that carries
 * routing metadata is removed with that metadata intact, and two requests for the same URN routed
 * to different underlying databases do not cross-clear.
 *
 * <p>{@code tick()} is {@code @Scheduled}; scheduling is turned on by {@code
 * RetentionBufferSchedulingConfig} (a gated {@code @EnableScheduling}) in ANY process that wires
 * the buffer — every GMS and MCE-consumer pod — not just the GMS analytics context. All pods share
 * one cluster-wide drain lock, so exactly one drains per tick regardless of pod count or type.
 * (Cluster discovery is Kubernetes-only; outside k8s each JVM is its own single-member cluster and
 * drains its own buffer — still safe via idempotent version-range DELETEs, just no cross-pod
 * coalescing.)
 *
 * <p>Drain-lock lease is not renewed mid-drain. If a batch of deletes exceeds the lease, another
 * pod may acquire the lock and drain concurrently; {@code removeIfSame} plus idempotent version-
 * range DELETEs prevent data loss (worst case: duplicate delete attempts).
 */
@Slf4j
public class RetentionDrainer {

  /** Sentinel lock name used for the single-winner drain lock; never a real retention key. */
  private static final String DRAIN_LOCK_NAME = "drain";

  private final CoalesceBuffer<RetentionKey, Long> buffer;
  private final RetentionService<?> retentionService;
  private final OperationContext systemOperationContext;
  private final RetentionContextResolver contextResolver;
  private final int batchSize;

  /** Safety-net lease so a drainer that dies mid-drain doesn't wedge the lock forever. */
  private final Duration drainLockLease;

  private final boolean enabled;
  @Nullable private final MetricUtils metricUtils;

  /**
   * Transient-resolver-failure backoff. When {@code groupKey} / {@code resolveOpContext} throws a
   * non-permanent {@link RuntimeException}, the key is removed from the buffer and re-merged after
   * {@link #TRANSIENT_BACKOFF_TICKS} ticks. Without this, a persistently-failing key would occupy
   * the first page of every {@link CoalesceBuffer#drain} (drain is non-destructive and returns the
   * same first page until {@code removeIfSame} clears it), starving every key behind it. With
   * backoff, the failing key is out of the buffer during the backoff window so {@code drain}
   * returns the keys behind it and they progress. The key is re-merged at the start of the next
   * tick once its backoff expires, so a transient blip self-heals and a persistent failure just
   * retries at a slower rate rather than wedging the drainer.
   */
  static final long TRANSIENT_BACKOFF_TICKS = 5L;

  private long currentTick = 0L;
  private final Map<RetentionKey, Long> transientRetryAt = new HashMap<>();
  private final Map<RetentionKey, Long> transientRetryVersion = new HashMap<>();

  public RetentionDrainer(
      @Nonnull CoalesceBuffer<RetentionKey, Long> buffer,
      @Nonnull RetentionService<?> retentionService,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull RetentionContextResolver contextResolver,
      int batchSize,
      long drainLockLeaseMs,
      boolean enabled,
      @Nullable MetricUtils metricUtils) {
    this.buffer = buffer;
    this.retentionService = retentionService;
    this.systemOperationContext = systemOperationContext;
    this.contextResolver = contextResolver;
    this.batchSize = batchSize;
    this.drainLockLease = Duration.ofMillis(drainLockLeaseMs);
    this.enabled = enabled;
    this.metricUtils = metricUtils;
  }

  // Default shared with RetentionBufferProperties.DEFAULT_DRAIN_INTERVAL_MS so the placeholder
  // fallback and the POJO field default can't drift. (Scheduling gate:
  // RetentionBufferSchedulingConfig.)
  @Scheduled(
      fixedDelayString =
          "${datahub.retention.buffer.drainIntervalMs:"
              + RetentionBufferProperties.DEFAULT_DRAIN_INTERVAL_MS
              + "}")
  public void tick() {
    if (!enabled) {
      return;
    }
    Object lockToken = buffer.tryAcquireDrainLock(DRAIN_LOCK_NAME, drainLockLease);
    if (lockToken == null) {
      // Another pod already won the drain lock for this tick.
      return;
    }
    try {
      long startMs = System.currentTimeMillis();
      drainBatch();
      long elapsedMs = System.currentTimeMillis() - startMs;
      // Lease is not renewed mid-drain: if a drain outlasts the lease, another pod can acquire the
      // lock and drain the same keys concurrently. That is safe (removeIfSame + idempotent
      // version-range DELETEs), just wasted work — surface it so operators can raise
      // drainLockLeaseMs or lower drainBatchSize.
      if (elapsedMs > drainLockLease.toMillis()) {
        log.warn(
            "Retention drain took {}ms, exceeding the {}ms lock lease; another pod may drain"
                + " concurrently (safe but wasteful). Raise datahub.retention.buffer.drainLockLeaseMs"
                + " or lower drainBatchSize.",
            elapsedMs,
            drainLockLease.toMillis());
        if (metricUtils != null) {
          metricUtils.increment(RetentionDrainer.class, "retention_drain_exceeded_lease", 1);
        }
      }
    } finally {
      buffer.releaseDrainLock(DRAIN_LOCK_NAME, lockToken);
    }
  }

  private void drainBatch() {
    currentTick++;
    // Re-merge keys whose transient backoff has expired so they retry on this tick. They were
    // removed from the buffer when the transient failure was caught, so drain() will now surface
    // them again alongside any other queued keys.
    if (!transientRetryAt.isEmpty()) {
      Iterator<Map.Entry<RetentionKey, Long>> it = transientRetryAt.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<RetentionKey, Long> e = it.next();
        if (e.getValue() <= currentTick) {
          Long version = transientRetryVersion.remove(e.getKey());
          it.remove();
          if (version != null) {
            buffer.merge(e.getKey(), version, CoalesceBuffers.KEEP_MAX_LONG);
          }
        }
      }
    }

    List<Map.Entry<RetentionKey, Long>> batch = buffer.drain(batchSize);
    if (batch.isEmpty()) {
      return;
    }

    // Build retention contexts up front, keyed by the ORIGINAL drained key (kept for the
    // removeIfSame clear step — a RetentionKey subtype that carries routing metadata must be
    // removed with that metadata intact, so we never reconstruct keys here). Malformed URNs are
    // cleared immediately so they don't wedge the drainer forever.
    Map<RetentionKey, RetentionService.RetentionContext> contextsByKey = new LinkedHashMap<>();
    for (Map.Entry<RetentionKey, Long> entry : batch) {
      try {
        Urn urn = Urn.createFromString(entry.getKey().urn());
        RetentionService.RetentionContext context =
            RetentionService.RetentionContext.builder()
                .urn(urn)
                .aspectName(entry.getKey().aspectName())
                .maxVersion(Optional.of(entry.getValue()))
                .build();
        contextsByKey.put(entry.getKey(), context);
      } catch (Exception e) {
        // Malformed URN in the buffer — clear it so it doesn't wedge the drainer forever.
        log.warn(
            "Skipping malformed retention key urn={} aspect={}; removing from buffer",
            entry.getKey().urn(),
            entry.getKey().aspectName(),
            e);
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
    }
    if (contextsByKey.isEmpty()) {
      return;
    }

    // Group entries by the resolver's routing grouping key so entries that share a routing context
    // are applied in one batch call. One OperationContext is reconstructed per group (from a
    // representative key) and used for every entry in that group's apply call.
    // LinkedHashMap (not HashMap) to preserve group insertion order across runs so drain order is
    // deterministic for reproducible logs/debugging.
    //
    // Resolver failures are split by intent:
    //  - UnresolvableRetentionKeyException = permanent (wrong key subtype / wiring bug / stale
    //    rolling-deploy entry): drop via removeIfSame so it doesn't re-throw every tick.
    //  - any other RuntimeException = transient (e.g. a temporary lookup error): move the key to a
    //    backoff limbo (removed from the buffer so other queued keys progress) and re-merge it
    //    after TRANSIENT_BACKOFF_TICKS ticks for retry. Dropping on a transient failure would
    //    silently skip retention for that entry; leaving it in-place would starve the keys behind
    //    it (drain is non-destructive and returns the same first page until removeIfSame clears).
    Map<RetentionKey, Long> versionByKey = new LinkedHashMap<>();
    for (Map.Entry<RetentionKey, Long> entry : batch) {
      versionByKey.put(entry.getKey(), entry.getValue());
    }
    Map<String, List<RetentionKey>> groups = new LinkedHashMap<>();
    for (RetentionKey key : contextsByKey.keySet()) {
      try {
        groups.computeIfAbsent(contextResolver.groupKey(key), k -> new ArrayList<>()).add(key);
      } catch (UnresolvableRetentionKeyException e) {
        log.warn(
            "Dropping unresolvable retention key urn={} aspect={}; removing from buffer",
            key.urn(),
            key.aspectName(),
            e);
        buffer.removeIfSame(key, versionByKey.get(key));
      } catch (RuntimeException e) {
        log.warn(
            "Transient resolver failure for retention key urn={} aspect={}; backing off for {}"
                + " ticks and leaving it out of the buffer so other queued keys progress",
            key.urn(),
            key.aspectName(),
            TRANSIENT_BACKOFF_TICKS,
            e);
        buffer.removeIfSame(key, versionByKey.get(key));
        transientRetryAt.put(key, currentTick + TRANSIENT_BACKOFF_TICKS);
        transientRetryVersion.put(key, versionByKey.get(key));
      }
    }

    Set<RetentionKey> successes = new HashSet<>();
    for (List<RetentionKey> groupKeys : groups.values()) {
      RetentionKey representative = groupKeys.get(0);
      OperationContext groupOpContext;
      try {
        groupOpContext = contextResolver.resolveOpContext(representative, systemOperationContext);
      } catch (UnresolvableRetentionKeyException e) {
        log.warn(
            "Dropping retention group of {} unresolvable keys; removing from buffer",
            groupKeys.size(),
            e);
        for (RetentionKey key : groupKeys) {
          buffer.removeIfSame(key, versionByKey.get(key));
        }
        continue;
      } catch (RuntimeException e) {
        log.warn(
            "Transient resolver failure for retention group of {} keys; backing off for {} ticks"
                + " and leaving them out of the buffer so other queued keys progress",
            groupKeys.size(),
            TRANSIENT_BACKOFF_TICKS,
            e);
        for (RetentionKey key : groupKeys) {
          buffer.removeIfSame(key, versionByKey.get(key));
          transientRetryAt.put(key, currentTick + TRANSIENT_BACKOFF_TICKS);
          transientRetryVersion.put(key, versionByKey.get(key));
        }
        continue;
      }
      List<RetentionBatchEntry> groupEntries = new ArrayList<>(groupKeys.size());
      for (RetentionKey key : groupKeys) {
        groupEntries.add(new RetentionBatchEntry(key, contextsByKey.get(key)));
      }
      try {
        successes.addAll(
            retentionService.applyRetentionBatchWithPolicyDefaults(groupOpContext, groupEntries));
      } catch (Exception e) {
        // Whole group failed (tx setup / commit). Nothing durable — the attempted keys stay for
        // retry (malformed keys were already removed above).
        log.warn(
            "Retention group apply failed for {} keys; leaving for retry", groupEntries.size(), e);
        if (metricUtils != null) {
          metricUtils.increment(RetentionDrainer.class, "retention_drain_failed", 1);
        }
      }
    }

    if (metricUtils != null) {
      metricUtils.increment(RetentionDrainer.class, "retention_drained", successes.size());
    }
    // Drained a full batch → the buffer likely holds more; the drainer may be falling behind the
    // enqueue rate. Operators watch this to decide whether to raise drainBatchSize / lower
    // interval.
    if (batch.size() == batchSize) {
      log.info(
          "Retention drain hit the batch cap ({}); buffer may be filling faster than it drains",
          batchSize);
    }

    // Clear only the keys whose commits were durably committed. removeIfSame guards against a
    // higher version having re-merged into the buffer while we were draining. Match by
    // RetentionKey equals (explicit per subtype) so a routing-metadata-carrying key subtype is
    // removed with its metadata intact, and two requests for the same URN routed to different
    // underlying databases do not cross-clear.
    for (Map.Entry<RetentionKey, Long> entry : batch) {
      if (successes.contains(entry.getKey())) {
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
    }
  }
}
