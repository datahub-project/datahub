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
   * "${datahub.retention.buffer.drainIntervalMs:5000}")}. Keep the YAML key and this field in sync
   * with that placeholder.
   */
  @Builder.Default private long drainIntervalMs = 5000;
}
