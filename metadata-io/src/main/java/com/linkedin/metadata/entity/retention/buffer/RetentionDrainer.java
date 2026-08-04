package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Background drainer over a {@link CoalesceBuffer} of pending retention keys. With the Hazelcast
 * backend exactly one pod cluster-wide applies retention per tick (shared drain lock) and the rest
 * no-op; with the local Caffeine backend each pod drains its own buffer independently. The drained
 * batch is handed to {@link RetentionService#applyRetentionBatchWithPolicyDefaults} in a single
 * database transaction (one commit per tick); per-pair savepoint isolation inside that tx means a
 * poison (urn, aspect) pair rolls back only its own DELETE. Keys whose contexts are returned as
 * committed are cleared via {@code removeIfSame}; everything else stays for the next tick to retry
 * (no retention_dlq table in v1 — see plan Global Constraints).
 *
 * <p>{@code tick()} is {@code @Scheduled}; scheduling is turned on by {@code
 * RetentionBufferSchedulingConfig} (a gated {@code @EnableScheduling}) in ANY process that wires
 * the buffer — every GMS and MCE-consumer pod — not just the GMS analytics context. With the
 * Hazelcast backend all pods share one cluster-wide drain lock, so exactly one drains per tick
 * regardless of pod count or type; with the local Caffeine backend each pod simply drains its own
 * local buffer.
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
  private final int batchSize;

  /** Safety-net lease so a drainer that dies mid-drain doesn't wedge the lock forever. */
  private final Duration drainLockLease;

  private final boolean enabled;
  @Nullable private final MetricUtils metricUtils;

  public RetentionDrainer(
      @Nonnull CoalesceBuffer<RetentionKey, Long> buffer,
      @Nonnull RetentionService<?> retentionService,
      @Nonnull OperationContext systemOperationContext,
      int batchSize,
      long drainLockLeaseMs,
      boolean enabled,
      @Nullable MetricUtils metricUtils) {
    this.buffer = buffer;
    this.retentionService = retentionService;
    this.systemOperationContext = systemOperationContext;
    this.batchSize = batchSize;
    this.drainLockLease = Duration.ofMillis(drainLockLeaseMs);
    this.enabled = enabled;
    this.metricUtils = metricUtils;
  }

  // Scheduling is enabled by RetentionBufferSchedulingConfig (@EnableScheduling gated on the same
  // buffer flags), so this fires in every process that wires the buffer — GMS and MCE-consumer pods
  // alike — not just the GMS analytics context. Interval from the property, not a POJO field read.
  @Scheduled(fixedDelayString = "${datahub.retention.buffer.drainIntervalMs:5000}")
  public void tick() {
    if (!enabled) {
      return;
    }
    if (!buffer.tryAcquireDrainLock(DRAIN_LOCK_NAME, drainLockLease)) {
      // Another pod already won the drain lock for this tick.
      return;
    }
    try {
      drainBatch();
    } finally {
      buffer.releaseDrainLock(DRAIN_LOCK_NAME);
    }
  }

  private void drainBatch() {
    List<Map.Entry<RetentionKey, Long>> batch = buffer.drain(batchSize);
    if (batch.isEmpty()) {
      return;
    }

    // Build retention contexts up front so the whole batch is applied in one tx (one fsync).
    List<RetentionService.RetentionContext> contexts = new ArrayList<>(batch.size());
    for (Map.Entry<RetentionKey, Long> entry : batch) {
      try {
        Urn urn = Urn.createFromString(entry.getKey().getUrn());
        contexts.add(
            RetentionService.RetentionContext.builder()
                .urn(urn)
                .aspectName(entry.getKey().getAspectName())
                .maxVersion(Optional.of(entry.getValue()))
                .build());
      } catch (Exception e) {
        // Malformed URN in the buffer — clear it so it doesn't wedge the drainer forever.
        log.warn(
            "Skipping malformed retention key urn={} aspect={}; removing from buffer",
            entry.getKey().getUrn(),
            entry.getKey().getAspectName(),
            e);
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
    }
    if (contexts.isEmpty()) {
      return;
    }

    List<RetentionService.RetentionContext> successes;
    try {
      successes =
          retentionService.applyRetentionBatchWithPolicyDefaults(systemOperationContext, contexts);
    } catch (Exception e) {
      // Whole batch failed (tx setup / commit). Nothing durable — the attempted contexts stay for
      // retry (malformed keys were already removed above, so count off contexts, not batch).
      log.warn("Retention batch apply failed; leaving {} keys for retry", contexts.size(), e);
      if (metricUtils != null) {
        metricUtils.increment(RetentionDrainer.class, "retention_drain_failed", 1);
      }
      return;
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

    // Clear only the keys whose contexts were durably committed. removeIfSame guards against a
    // higher version having re-merged into the buffer while we were draining.
    Set<RetentionKey> successKeys =
        successes.stream()
            .map(ctx -> new RetentionKey(ctx.getUrn().toString(), ctx.getAspectName()))
            .collect(Collectors.toSet());
    for (Map.Entry<RetentionKey, Long> entry : batch) {
      if (successKeys.contains(entry.getKey())) {
        buffer.removeIfSame(entry.getKey(), entry.getValue());
      }
    }
  }
}
