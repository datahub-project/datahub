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
  private String securityProtocol;
  private String schemaRegistryUrl;

  /** Total producer construction attempts, including the first try */
  private int initializationRetryCount = 5;

  /** Initial backoff delay in milliseconds for producer initialization retries */
  private long initializationRetryBackoffMs = 500;

  /** Maximum backoff delay in milliseconds between producer initialization retries */
  private long initializationRetryMaxBackoffMs = 4000;

  /** Maximum cumulative wait time in milliseconds across producer initialization retries */
  private long initializationRetryMaxTotalWaitMs = 15000;
}
