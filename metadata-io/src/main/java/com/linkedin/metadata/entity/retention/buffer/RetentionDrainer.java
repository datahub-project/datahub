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
 * Scheduled background drainer over a {@link CoalesceBuffer} of pending retention keys. Only one
 * GMS pod applies retention per tick (drain lock); the rest no-op. The whole drained batch is
 * handed to {@link RetentionService#applyRetentionBatchWithPolicyDefaults} in a single database
 * transaction (one commit per tick); per-pair savepoint isolation inside that tx means a poison
 * (urn, aspect) pair rolls back only its own DELETE. Keys whose contexts are returned as committed
 * are cleared via {@code removeIfSame}; everything else stays for the next tick to retry (no
 * retention_dlq table in v1 — see plan Global Constraints).
 *
 * <p>Requires Spring {@code @EnableScheduling} in the owning context (GMS enables this via {@code
 * ScheduledAnalyticsFactory}). Without it, {@link #tick} never runs and pending keys sit until
 * MapConfig eviction — missed prune = bloat, not data loss.
 *
 * <p>Drain-lock lease ({@link #DRAIN_LOCK_LEASE}) is not renewed mid-drain. If a batch of deletes
 * exceeds the lease, another pod may acquire the lock and drain concurrently; {@code removeIfSame}
 * prevents data loss (worst case: duplicate delete attempts).
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

  // Interval from Spring env / YAML — not RetentionBufferProperties.drainIntervalMs field reads.
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
      // Whole batch failed (tx setup / commit). Nothing durable — all keys stay for retry.
      log.warn("Retention batch apply failed; leaving all {} keys for retry", batch.size(), e);
      if (metricUtils != null) {
        metricUtils.increment(RetentionDrainer.class, "retention_drain_failed", 1);
      }
      return;
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
