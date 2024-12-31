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
}
