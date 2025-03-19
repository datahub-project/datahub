package com.linkedin.metadata.config;

import lombok.Data;

/** Configurations related to Executor Pool SQS Provisioning Hook. */
@Data
public class ExecutorPoolHookConfiguration {
  public boolean enabled;

  public String messageRetentionPeriod;

  public String managedSseEnabled;

  public String visibilityTimeout;

  public String consumerGroupSuffix;

  public int sqsMaxRetries;

  public int sqsBaseDelayMs;

  public String policyTemplate;
}
