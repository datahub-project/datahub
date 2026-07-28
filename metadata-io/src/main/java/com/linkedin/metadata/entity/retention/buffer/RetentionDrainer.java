package com.linkedin.metadata.entity.retention.buffer;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Scheduled background drainer over a {@link CoalesceBuffer} of pending retention keys. Only one
 * GMS pod applies retention per tick (drain lock); the rest no-op. Per-key apply so one bad delete
 * never blocks the batch, and a failed key is simply left in the buffer for the next tick to retry
 * (no retention_dlq table in v1 — see plan Global Constraints).
 */
@Slf4j
public class RetentionDrainer {

  /** Sentinel lock name used for the single-winner drain lock; never a real retention key. */
  private static final String DRAIN_LOCK_NAME = "drain";

  /** Safety-net lease so a drainer that dies mid-drain doesn't wedge the lock forever. */
  private static final Duration DRAIN_LOCK_LEASE = Duration.ofSeconds(60);

  private final CoalesceBuffer<RetentionKey, Long> buffer;
  private final RetentionService<?> retentionService;
  private final OperationContext systemOperationContext;
  private final int batchSize;
  private final boolean enabled;
  @Nullable private final MetricUtils metricUtils;

  public RetentionDrainer(
      @Nonnull CoalesceBuffer<RetentionKey, Long> buffer,
      @Nonnull RetentionService<?> retentionService,
      @Nonnull OperationContext systemOperationContext,
      int batchSize,
      boolean enabled,
      @Nullable MetricUtils metricUtils) {
    this.buffer = buffer;
    this.retentionService = retentionService;
    this.systemOperationContext = systemOperationContext;
    this.batchSize = batchSize;
    this.enabled = enabled;
    this.metricUtils = metricUtils;
  }

  @Scheduled(fixedDelayString = "${datahub.retention.buffer.drainIntervalMs:5000}")
  public void tick() {
    if (!enabled) {
      return;
    }
    if (!buffer.tryAcquireDrainLock(DRAIN_LOCK_NAME, DRAIN_LOCK_LEASE)) {
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
    for (Map.Entry<RetentionKey, Long> entry : batch) {
      drainOne(entry.getKey(), entry.getValue());
    }
  }

  private void drainOne(@Nonnull RetentionKey key, long version) {
    try {
      Urn urn = Urn.createFromString(key.getUrn());
      RetentionService.RetentionContext ctx =
          RetentionService.RetentionContext.builder()
              .urn(urn)
              .aspectName(key.getAspectName())
              .maxVersion(Optional.of(version))
              .build();
      retentionService.applyRetentionWithPolicyDefaults(systemOperationContext, List.of(ctx));
      // Only clear the key if nothing re-merged a higher version while we were draining it.
      buffer.removeIfSame(key, version);
    } catch (Exception e) {
      log.warn(
          "Retention drain failed for urn={} aspect={}; leaving key for retry",
          key.getUrn(),
          key.getAspectName(),
          e);
      if (metricUtils != null) {
        metricUtils.increment(RetentionDrainer.class, "retention_drain_failed", 1);
      }
    }
  }
}
