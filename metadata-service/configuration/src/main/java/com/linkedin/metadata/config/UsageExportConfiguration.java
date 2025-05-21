package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class UsageExportConfiguration {
  private boolean enabled;
  private String usageEventTypes;
  private String aspectTypes;
  private String userFilters;
}
