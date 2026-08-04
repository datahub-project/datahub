package com.linkedin.metadata.config.retention;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO representing the "datahub.retention.buffer" tuning block in application.yaml (map names,
 * batch sizes, drain interval). On/off is {@code featureFlags.retentionBufferEnabled}, not this
 * POJO. Also requires {@code featureFlags.postCommitRetentionEnabled}; backend is {@code
 * datahub.buffer.implementation} (caffeine | hazelcast), else sync DELETE post-commit.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RetentionBufferProperties {
  @Builder.Default private String mapName = "retention-pending";
  @Builder.Default private String lockMapName = "retention-drain-lock";
  @Builder.Default private int maxPendingEntries = 100_000;
  @Builder.Default private int drainBatchSize = 500;

  /**
   * Bound from {@code datahub.retention.buffer.drainIntervalMs} for config surface /
   * PropertiesCollector. Not read by Java callers of this POJO — {@code RetentionDrainer} consumes
   * the same Spring property key via {@code @Scheduled(fixedDelayString =
   * "${datahub.retention.buffer.drainIntervalMs:5000}")}. The {@code 5000} default is duplicated in
   * both places (the {@code @Scheduled} placeholder must be a compile-time literal, and this POJO
   * lives in a different module) — no shared constant is possible, so keep both in sync by hand.
   */
  @Builder.Default private long drainIntervalMs = 5000;

  /**
   * Safety-net lease on the single-winner drain lock so a drainer that dies mid-drain does not
   * wedge it forever. Must exceed a drain's worst-case duration; if a drain routinely runs longer,
   * ticks double-drain (safe/idempotent, just wasteful). Raise alongside {@code drainBatchSize}
   * under DB latency. Bound from {@code datahub.retention.buffer.drainLockLeaseMs}.
   */
  @Builder.Default private long drainLockLeaseMs = 60_000;
}
