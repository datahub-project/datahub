package com.linkedin.metadata.config.kafka;

import lombok.Data;

@Data
public class ConsumerConfiguration {

  private int maxPartitionFetchBytes;
  private int maxPollRecords;
  private int maxPollIntervalMs;
  private boolean stopOnDeserializationError;
  private boolean healthCheckEnabled;
}
