package com.linkedin.metadata.config;

import com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration;
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

  private ConsumerBatchConfig consumer;
  private ThrottleConfig throttle;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ThrottleConfig {
    private TimeseriesWriteThrottleConfiguration timeseries;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ConsumerBatchConfig {
    private BatchConfig batch;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class BatchConfig {
    private boolean enabled;
    private Integer size;
  }
}
