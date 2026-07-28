package com.linkedin.metadata.config.retention;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO representing the "datahub.retention.buffer" tuning block in application.yaml (map names,
 * batch sizes). On/off is {@code featureFlags.retentionBufferEnabled}, not this POJO. Requires
 * {@code featureFlags.postCommitRetentionEnabled} and Hazelcast, else sync DELETE post-commit.
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
  @Builder.Default private long drainIntervalMs = 5000;
}
