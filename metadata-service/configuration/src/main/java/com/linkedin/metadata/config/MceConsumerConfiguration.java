package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Runtime tuning for {@code metadata-jobs/mce-consumer-job}. Defaults and env-backed placeholders
 * live only in {@code application.yaml} ({@code mceConsumer.*}).
 */
@Data
public class MceConsumerConfiguration {

  private PgQueuePoll pgQueue;

  /** pgQueue SQL poll worker settings for MCE consumer pipelines. */
  @Data
  public static class PgQueuePoll {
    /**
     * Max rows per poll for single-record MCP processing ({@code MetadataChangeProposalConsumer}).
     */
    private Integer metadataChangeProposalMaxBatch;

    /**
     * Max rows per poll for batch MCP processing ({@code BatchMetadataChangeProposalsProcessor}).
     */
    private Integer batchMetadataChangeProposalMaxBatch;
  }
}
