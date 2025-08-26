package com.linkedin.metadata.config.aws;

import lombok.Data;

@Data
public class EventBridgeConfiguration {
  private String eventBus;
  private String region;
  private String assumeRoleArn;
  private String externalId;
  private int maxBatchSize;
  private int maxRetries;
  private int flushIntervalSeconds;
  private AuditEventExportConfiguration auditEventExport;
}
