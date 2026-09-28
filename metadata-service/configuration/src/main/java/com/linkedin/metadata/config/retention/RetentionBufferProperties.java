package com.linkedin.metadata.config.retention;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO representing the "datahub.retention.buffer" tuning block in application.yaml (map names,
 * batch sizes, drain interval). On/off is {@code featureFlags.retentionBufferEnabled}, not this
 * POJO. Also requires {@code featureFlags.postCommitRetentionEnabled}; the buffer is backed by the
 * shared embedded Hazelcast instance, else sync DELETE post-commit.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RetentionBufferProperties {

  /**
   * Single source of the drain-interval default. Referenced both by this POJO's {@code
   * drainIntervalMs} field and by {@code RetentionDrainer}'s {@code @Scheduled} placeholder (as a
   * compile-time constant, which the annotation requires), so the two can never drift.
   * (String-typed because the {@code @Scheduled} placeholder needs a String constant.)
   */
  public static final String DEFAULT_DRAIN_INTERVAL_MS = "5000";

  @Builder.Default private String mapName = "retention-pending";
  @Builder.Default private String lockMapName = "retention-drain-lock";
  @Builder.Default private int maxPendingEntries = 100_000;
  @Builder.Default private int drainBatchSize = 500;

  /**
   * Bound from {@code datahub.retention.buffer.drainIntervalMs} for config surface /
   * PropertiesCollector. Not read by Java callers of this POJO — {@code RetentionDrainer} consumes
   * the same Spring property key via {@code @Scheduled}. Both defaults come from {@link
   * #DEFAULT_DRAIN_INTERVAL_MS}. (The application.yaml default is separate and authoritative at
   * runtime; these are only the code fallbacks when the property is unset.)
   */
  @Builder.Default private long drainIntervalMs = Long.parseLong(DEFAULT_DRAIN_INTERVAL_MS);

  /**
   * Safety-net lease on the single-winner drain lock so a drainer that dies mid-drain does not
   * wedge it forever. Must exceed a drain's worst-case duration; if a drain routinely runs longer,
   * ticks double-drain (safe/idempotent, just wasteful). Raise alongside {@code drainBatchSize}
   * under DB latency. Bound from {@code datahub.retention.buffer.drainLockLeaseMs}.
   */
  @Builder.Default private long drainLockLeaseMs = 60_000;
}
