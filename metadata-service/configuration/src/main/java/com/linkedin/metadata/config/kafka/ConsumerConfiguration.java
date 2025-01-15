package com.linkedin.metadata.config.kafka;

import lombok.Data;

@Data
public class ConsumerConfiguration {

  private int maxPartitionFetchBytes;
  private boolean stopOnDeserializationError;
  private boolean healthCheckEnabled;

  private ConsumerOptions mcp;
  private ConsumerOptions mcl;
  private ConsumerOptions pe;

  @Data
  public static class ConsumerOptions {
    private String autoOffsetReset;
  }
}
