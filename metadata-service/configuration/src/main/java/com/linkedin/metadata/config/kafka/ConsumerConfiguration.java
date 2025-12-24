package com.linkedin.metadata.config.kafka;

import com.linkedin.metadata.config.MetricsOptions;
import lombok.Data;

@Data
public class ConsumerConfiguration {

  private int maxPartitionFetchBytes;
  private boolean stopOnDeserializationError;
  private boolean healthCheckEnabled;

  private ConsumerOptions mcp;
  private ConsumerOptions mcl;
  private ConsumerOptions pe;

  private MetricsOptions metrics;
  private String bootstrapServers;

  @Data
  public static class ConsumerOptions {
    private String autoOffsetReset;
    private boolean fineGrainedLoggingEnabled;
    private String aspectsToDrop;
  }
}
