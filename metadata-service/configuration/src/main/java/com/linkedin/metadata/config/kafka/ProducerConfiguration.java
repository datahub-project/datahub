package com.linkedin.metadata.config.kafka;

import lombok.Data;

@Data
public class ProducerConfiguration {

  private int retryCount;

  private int deliveryTimeout;

  private int requestTimeout;

  private int backoffTimeout;

  private String compressionType;

  private int maxRequestSize;

  private String bootstrapServers;

  /** Number of retries for producer initialization */
  private int initializationRetryCount = 5;

  /** Initial backoff delay in milliseconds for producer initialization retries */
  private long initializationRetryBackoffMs = 500;
}
