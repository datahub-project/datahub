package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Configuration for Metadata Change Log (MCL) processing. Similar to MetadataChangeProposalConfig
 * but for MCL events.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class MetadataChangeLogConfig {

  /** Consumer configuration for MCL processing */
  private ConsumerBatchConfig consumer;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ConsumerBatchConfig {
    /** Batch processing configuration */
    private BatchConfig batch;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class BatchConfig {
    /** Whether batch processing is enabled */
    private boolean enabled;

    /** Maximum batch size in bytes */
    private Integer size;
  }
}
