package com.linkedin.metadata.config.search;

import javax.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@Builder(toBuilder = true)
@Slf4j
public class BuildIndicesConfiguration {

  /** Default socket timeout in seconds for slow index operations. */
  public static final int DEFAULT_SLOW_OPERATION_TIMEOUT_SECONDS = 180;

  /** Default max attempts for getCount retry. */
  public static final int DEFAULT_COUNT_RETRY_MAX_ATTEMPTS = 2;

  /** Default wait in seconds between getCount retries. */
  public static final int DEFAULT_COUNT_RETRY_WAIT_SECONDS = 5;

  private boolean cloneIndices;
  private boolean allowDocCountMismatch;
  private String retentionUnit;
  private Long retentionValue;
  private boolean reindexOptimizationEnabled;

  /** Reindex source batch size (documents per scroll batch). Default 5000. */
  private Integer reindexBatchSize;

  /** Maximum number of slices for reindex (capped from target shard count). Default 256. */
  private Integer reindexMaxSlices;

  /** Minutes without document-count progress before re-triggering reindex. Default 5. */
  private Integer reindexNoProgressRetryMinutes;

  // Parallel reindexing configuration
  @Builder.Default private boolean enableParallelReindex = false;

  @Builder.Default private int taskCheckIntervalSeconds = 15;

  @Builder.Default private int maxReindexHours = 12;

  @Builder.Default private long normalIndexCostThreshold = 500_000;

  // Maximum concurrent reindex jobs for NORMAL tier indices
  @Builder.Default private int maxConcurrentNormalReindex = 4;

  // Maximum concurrent reindex jobs for LARGE tier indices (should be 1)
  @Builder.Default private int maxConcurrentLargeReindex = 2;

  // Cluster health monitoring interval (seconds) for throttling
  @Builder.Default private int clusterHealthCheckIntervalSeconds = 30;

  // Cluster health thresholds for auto-throttling
  @Builder.Default private int clusterHeapThresholdPercent = 90;

  // Heap threshold for YELLOW tier (triggers throttling before RED)
  @Builder.Default private int clusterHeapYellowThresholdPercent = 75;

  // Maximum concurrent finalizations for reindexed indices (8 for testing)
  // Limits parallel replica ramps to avoid cluster IO spike
  @Builder.Default private int maxConcurrentFinalizations = 5;

  // Timeout (minutes) for waiting on replica sync before promotion (default 1)
  @Builder.Default private int replicaSyncTimeoutMinutes = 1;

  // Doc count validation retry configuration
  @Builder.Default private int docCountValidationRetryCount = 10;

  @Builder.Default private int docCountValidationRetrySleepMs = 2000;

  // Adaptive throttling configuration for request rate limiting
  // Environment variable: ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REQUESTS_PER_SECOND
  // Requests per second during NORMAL cluster health tier (default 500)
  @Builder.Default private int normalTierRequestsPerSecond = 500;

  // Environment variable: ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REQUESTS_PER_SECOND
  // Requests per second during THROTTLED cluster health tier (default 100)
  @Builder.Default private int throttledTierRequestsPerSecond = 100;

  // Dynamic refresh_interval configuration for adaptive memory management
  // Environment variable: ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REFRESH_INTERVAL
  // Refresh interval during NORMAL cluster health tier, allows periodic flushes to reduce heap
  // pressure
  // (default "60s")
  @Builder.Default private String normalTierRefreshInterval = "60s";

  // Environment variable: ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REFRESH_INTERVAL
  // Refresh interval during THROTTLED (RED) cluster health tier, aggressive flush for heap relief
  // (default "30s")
  @Builder.Default private String throttledTierRefreshInterval = "30s";

  // Thread pool rejection detection configuration
  // Environment variable: ELASTICSEARCH_BUILD_INDICES_WRITE_REJECTION_RED_THRESHOLD
  // Delta threshold for write/bulk thread pool rejections to trigger immediate RED state
  // RED is triggered when 2 consecutive polls both exceed this threshold (sustained rejection
  // spike)
  // (default 50 rejections per poll)
  @Builder.Default private int writeRejectionRedThreshold = 50;

  // Circuit breaker stability window configuration (time-based hysteresis)
  // These prevent flapping between health states by requiring candidate states to be stable
  // for the configured duration before transitioning.

  // Environment variable: ELASTICSEARCH_BUILD_INDICES_YELLOW_STABILITY_SECONDS
  // Time (seconds) for GREEN -> YELLOW transition to be stable before accepting (default 30)
  @Builder.Default private int yellowStabilitySeconds = 30;

  // Environment variable: ELASTICSEARCH_BUILD_INDICES_GREEN_STABILITY_SECONDS
  // Time (seconds) for YELLOW -> GREEN transition to be stable before accepting (default 30)
  @Builder.Default private int greenStabilitySeconds = 30;

  // Environment variable: ELASTICSEARCH_BUILD_INDICES_RED_RECOVERY_SECONDS
  // Time (seconds) for RED -> YELLOW transition to be stable before accepting (default 30)
  // Typically longer than other windows to avoid false recoveries
  @Builder.Default private int redRecoverySeconds = 30;

  // Parallel reindex monitoring and execution configuration
  // Environment variable: ELASTICSEARCH_BUILD_INDICES_RETHROTTLE_EXECUTOR_POOL_SIZE
  // Size of thread pool for concurrent rethrottle operations (default 8)
  @Builder.Default private int rethrottleExecutorPoolSize = 8;

  // Environment variable: ELASTICSEARCH_BUILD_INDICES_MINIMUM_REPLICAS_FOR_PROMOTION
  // Minimum number of replicas to restore before promoting index (default 1)
  @Builder.Default private int minimumReplicasForPromotion = 1;

  /** Validate configuration parameters on bean creation */
  @PostConstruct
  public void validate() {
    if (normalIndexCostThreshold < 0) {
      throw new IllegalArgumentException(
          "normalIndexCostThreshold must be >= 0, got: " + normalIndexCostThreshold);
    }
    if (maxConcurrentNormalReindex <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentNormalReindex must be > 0, got: " + maxConcurrentNormalReindex);
    }
    if (maxConcurrentLargeReindex <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentLargeReindex must be > 0, got: " + maxConcurrentLargeReindex);
    }
    if (taskCheckIntervalSeconds <= 0) {
      throw new IllegalArgumentException(
          "taskCheckIntervalSeconds must be > 0, got: " + taskCheckIntervalSeconds);
    }
    if (maxReindexHours <= 0) {
      throw new IllegalArgumentException(
          "maxReindexHours must be > 0 (positive), got: " + maxReindexHours);
    }
    if (maxConcurrentFinalizations <= 0 || maxConcurrentFinalizations > 100) {
      throw new IllegalArgumentException(
          "maxConcurrentFinalizations must be between 1 and 100, got: "
              + maxConcurrentFinalizations);
    }
    if (replicaSyncTimeoutMinutes < 0) {
      throw new IllegalArgumentException(
          "replicaSyncTimeoutMinutes must be >= 0, got: " + replicaSyncTimeoutMinutes);
    }
    if (docCountValidationRetryCount <= 0) {
      throw new IllegalArgumentException(
          "docCountValidationRetryCount must be > 0, got: " + docCountValidationRetryCount);
    }
    if (docCountValidationRetrySleepMs < 0) {
      throw new IllegalArgumentException(
          "docCountValidationRetrySleepMs must be >= 0, got: " + docCountValidationRetrySleepMs);
    }
    if (normalTierRequestsPerSecond <= 0) {
      throw new IllegalArgumentException(
          "normalTierRequestsPerSecond must be > 0, got: " + normalTierRequestsPerSecond);
    }
    if (throttledTierRequestsPerSecond <= 0) {
      throw new IllegalArgumentException(
          "throttledTierRequestsPerSecond must be > 0, got: " + throttledTierRequestsPerSecond);
    }
    if (writeRejectionRedThreshold < 0) {
      throw new IllegalArgumentException(
          "writeRejectionRedThreshold must be >= 0, got: " + writeRejectionRedThreshold);
    }
    if (clusterHeapThresholdPercent < 0 || clusterHeapThresholdPercent > 100) {
      throw new IllegalArgumentException(
          "clusterHeapThresholdPercent must be between 0 and 100, got: "
              + clusterHeapThresholdPercent);
    }
    if (clusterHeapYellowThresholdPercent < 0 || clusterHeapYellowThresholdPercent > 100) {
      throw new IllegalArgumentException(
          "clusterHeapYellowThresholdPercent must be between 0 and 100, got: "
              + clusterHeapYellowThresholdPercent);
    }
    if (clusterHealthCheckIntervalSeconds <= 0) {
      throw new IllegalArgumentException(
          "clusterHealthCheckIntervalSeconds must be > 0, got: "
              + clusterHealthCheckIntervalSeconds);
    }
    if (yellowStabilitySeconds <= 0) {
      throw new IllegalArgumentException(
          "yellowStabilitySeconds must be > 0, got: " + yellowStabilitySeconds);
    }
    if (greenStabilitySeconds <= 0) {
      throw new IllegalArgumentException(
          "greenStabilitySeconds must be > 0, got: " + greenStabilitySeconds);
    }
    if (redRecoverySeconds <= 0) {
      throw new IllegalArgumentException(
          "redRecoverySeconds must be > 0, got: " + redRecoverySeconds);
    }
    if (rethrottleExecutorPoolSize <= 0 || rethrottleExecutorPoolSize > 100) {
      throw new IllegalArgumentException(
          "rethrottleExecutorPoolSize must be between 1 and 100, got: "
              + rethrottleExecutorPoolSize);
    }
    if (minimumReplicasForPromotion < 0) {
      throw new IllegalArgumentException(
          "minimumReplicasForPromotion must be >= 0, got: " + minimumReplicasForPromotion);
    }
    log.info(
        "BuildIndicesConfiguration stability windows: yellowStability={}s, greenStability={}s, redRecovery={}s",
        yellowStabilitySeconds,
        greenStabilitySeconds,
        redRecoverySeconds);
  }

  /**
   * Socket timeout in seconds for slow index operations (count, refresh, createIndex, reindex,
   * listTasks, etc.). Set via application.yaml. Default {@link
   * #DEFAULT_SLOW_OPERATION_TIMEOUT_SECONDS}.
   */
  private int slowOperationTimeoutSeconds = DEFAULT_SLOW_OPERATION_TIMEOUT_SECONDS;

  /**
   * Max attempts for getCount retry on timeout/OpenSearchException. Set via application.yaml.
   * Default {@link #DEFAULT_COUNT_RETRY_MAX_ATTEMPTS}.
   */
  private int countRetryMaxAttempts = DEFAULT_COUNT_RETRY_MAX_ATTEMPTS;

  /**
   * Wait duration in seconds between getCount retries. Set via application.yaml. Default {@link
   * #DEFAULT_COUNT_RETRY_WAIT_SECONDS}.
   */
  private int countRetryWaitSeconds = DEFAULT_COUNT_RETRY_WAIT_SECONDS;

  /**
   * When true, createIndex retries once on IOException if index does not exist. Set via
   * application.yaml.
   */
  private boolean createIndexRetryEnabled = true;

  /**
   * When true, enables non-blocking incremental reindex. Instead of blocking writes and swapping
   * aliases in-place, creates 'next' indices, copies data via ES _reindex, and a catch-up step
   * running to get the next index in line with any missed writes
   */
  private boolean incrementalReindexEnabled;

  /**
   * When true (and incrementalReindexEnabled is also true), the MAE consumer dual-writes to the old
   * backing index for rollback safety. When false, Phase 2 marks all completed indices as
   * DUAL_WRITE_DISABLED to prevent a later enable from writing to stale or deleted old indices.
   */
  private boolean rollbackDualWriteEnabled;
}
