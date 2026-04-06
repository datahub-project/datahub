package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@Builder(toBuilder = true)
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
}
