package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Bulk writer tuning. This is the shared default at {@code elasticsearch.bulkProcessor}; a cluster
 * may overlay individual fields via {@code elasticsearch.clusters.<name>.bulkProcessor}. Two
 * clusters that resolve to the same endpoint but different merged settings get separate processors.
 */
@Slf4j
@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class BulkProcessorConfiguration {
  private boolean async;
  private int requestsLimit;
  private int flushPeriod;
  private int numRetries;
  private long retryInterval;
  private String refreshPolicy;
  private boolean enableBatchDelete;

  /** Bounded same-processor requeue of failed bulk items (version conflicts, 429/503, etc.). */
  private boolean itemRequeueEnabled;

  private int itemRequeueMaxAttempts;

  /** When true, MAE UpdateIndices awaits bulk transfer before acking Kafka / pgQueue. */
  private boolean ackAfterTransfer;

  private int ackAfterTransferTimeoutSeconds;

  /**
   * Socket timeout in seconds for OpenSearch {@code RequestOptions} on by-query class operations
   * (delete/update-by-query, etc.). Same value for GMS and MAE processes; MAE-only tuning applies
   * to RestClient ({@code maeConsumer.elasticsearch.*}), not this setting. Not used for
   * system-update build-indices / reindex flows—those use {@link BuildIndicesConfiguration}.
   */
  private int slowByQueryOperationTimeoutSeconds;
}
