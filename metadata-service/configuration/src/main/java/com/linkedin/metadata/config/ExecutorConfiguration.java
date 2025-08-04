package com.linkedin.metadata.config;

import lombok.Data;

/** Configurations related to Acryl executors. */
@Data
public class ExecutorConfiguration {
  /** The role ARN that we should assume for executor credentials . */
  public String executorRoleArn;

  /** The customer id we will use to filter customer SQS queues */
  public String executorCustomerId;

  /**
   * Backend revision number. This allows RE clients to know which features are vailable on the
   * server when making API calls. Every time a breaking change is introduced on the server side,
   * this should be incremented, and a corresponding check added on the client. *
   */
  public int backendRevision;

  /** Executor pool SQS provisioning hook config */
  public ExecutorPoolHookConfiguration executorPoolHook;

  /** S3 bucket name for cloud logging */
  public String cloudLoggingS3Bucket;

  /** S3 prefix for cloud logging */
  public String cloudLoggingS3Prefix;

  /** Whether remote executor logging is enabled or not */
  public boolean remoteExecutorLoggingEnabled;
}
