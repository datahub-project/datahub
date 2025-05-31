package com.linkedin.metadata.config.aws;

import lombok.Data;

@Data
public class AuditEventExportConfiguration {
  private boolean enabled;
  private String usageEventTypes;
  private String aspectTypes;
  private String userFilters;
  private String consumerGroupSuffix;
}
