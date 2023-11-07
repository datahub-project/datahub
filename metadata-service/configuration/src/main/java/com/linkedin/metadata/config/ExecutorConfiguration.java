package com.linkedin.metadata.config;

import lombok.Data;

/** Configurations related to Acryl executors. */
@Data
public class ExecutorConfiguration {
  /** The role ARN that we should assume for executor credentials credentials . */
  public String executorRoleArn;
  /** The customer id we will use to filter customer SQS queues */
  public String executorCustomerId;
}
