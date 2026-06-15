package com.linkedin.metadata.config;

import com.linkedin.metadata.config.shared.LimitConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class TimeseriesAspectServiceConfig {
  @Builder.Default private ExecutorServiceConfig query = ExecutorServiceConfig.builder().build();
  private LimitConfig limit;

  /**
   * When true, {@code getLatestTimeseriesAspectValues} and {@link
   * TimeseriesAspectService#batchGetAspectValues} use a single top_hits aggregation query per
   * (entityType, aspectName) group instead of fanning out one ES query per URN. Shared with the
   * {@code featureFlags.timeseriesAspectBatchLoadEnabled} flag via the same env var {@code
   * TIMESERIES_ASPECT_BATCH_LOAD_ENABLED}; both must be toggled together to fully disable the batch
   * path.
   */
  @Builder.Default private boolean batchLoadEnabled = true;

  /**
   * Maximum total documents (limit × URN count) allowed per batch top_hits aggregation query.
   * Larger values reduce the number of ES round-trips for big batches; smaller values reduce memory
   * pressure on the ES heap during aggregation. Default: 500.
   */
  @Builder.Default private int topHitsThreshold = 500;

  /**
   * Per-bucket (per-URN) top_hits size for batch aggregation queries. Defaults to 100 to match
   * OpenSearch's {@code index.max_inner_result_window} default. Raise this setting together with
   * {@code index.max_inner_result_window} on the timeseries indices if larger per-entity batch
   * windows are needed. Batch calls whose {@code limit} exceeds this value are routed to the
   * single-URN path automatically.
   */
  @Builder.Default private int topHitsPerBucketLimit = 100;
}
