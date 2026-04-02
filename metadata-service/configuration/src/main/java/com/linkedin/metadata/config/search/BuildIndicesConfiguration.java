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

  /**
   * Default ceiling for combined median MCL lag (versioned + timeseries) before auto alias swap.
   * Raise (e.g. to {@link Long#MAX_VALUE}) to effectively disable the gate.
   */
  public static final long DEFAULT_PRE_ALIAS_SWAP_MAX_MCL_LAG_TOTAL = 5000L;

  public static final long DEFAULT_PRE_ALIAS_SWAP_LAG_RETRY_INITIAL_BACKOFF_MS = 5000L;

  public static final long DEFAULT_PRE_ALIAS_SWAP_LAG_RETRY_MAX_BACKOFF_MS = 60_000L;

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

  /**
   * When true, enables non-blocking incremental reindex. Instead of blocking writes and swapping
   * aliases in-place, creates 'next' indices, copies data via ES _reindex, and relies on dual-write
   * from the MAE consumer to keep the next index current until alias swap.
   */
  private boolean incrementalReindexEnabled;

  /**
   * When true (and incrementalReindexEnabled is also true), automatically swaps the alias from the
   * current index to the next index once document counts converge. When false, alias swap must be
   * triggered manually via the operations API.
   */
  private boolean autoAliasSwapEnabled;

  /**
   * How often (in seconds) the MAE consumer's dual-write strategy polls the upgrade state to detect
   * indices marked as ALIAS_SWAPPED. Defaults to 300 seconds (5 minutes).
   */
  private long dualWritePollIntervalSeconds = 300;

  /**
   * Maximum combined median MCL lag (versioned + timeseries topics) before automatic alias swap.
   * Consumer group matches {@code METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID} / {@code
   * MCLKafkaListenerRegistrar}. Default {@link #DEFAULT_PRE_ALIAS_SWAP_MAX_MCL_LAG_TOTAL}. Set very
   * high (e.g. {@link Long#MAX_VALUE}) to effectively disable the gate.
   */
  private long preAliasSwapMaxMclLagTotal = DEFAULT_PRE_ALIAS_SWAP_MAX_MCL_LAG_TOTAL;

  /**
   * When lag exceeds {@link #preAliasSwapMaxMclLagTotal} (or Kafka admin fails), the pre-alias lag
   * step re-checks after exponential backoff; this is the number of <em>additional</em> attempts
   * after the first (total attempts = {@code 1 + preAliasSwapLagStepRetries}). Default {@code 5}.
   * Retries run inside the step (see {@code IncrementalReindexPreAliasSwapLagStep}); {@link
   * com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager} {@code retryCount} for this step is
   * {@code 0}.
   */
  private int preAliasSwapLagStepRetries = 5;

  /**
   * Milliseconds to wait before the second lag measurement; doubles each subsequent failure until
   * capped by {@link #preAliasSwapLagRetryMaxBackoffMs}. Default {@link
   * #DEFAULT_PRE_ALIAS_SWAP_LAG_RETRY_INITIAL_BACKOFF_MS}.
   */
  private long preAliasSwapLagRetryInitialBackoffMs =
      DEFAULT_PRE_ALIAS_SWAP_LAG_RETRY_INITIAL_BACKOFF_MS;

  /**
   * Maximum backoff between lag check attempts. Default {@link
   * #DEFAULT_PRE_ALIAS_SWAP_LAG_RETRY_MAX_BACKOFF_MS}.
   */
  private long preAliasSwapLagRetryMaxBackoffMs = DEFAULT_PRE_ALIAS_SWAP_LAG_RETRY_MAX_BACKOFF_MS;
}
