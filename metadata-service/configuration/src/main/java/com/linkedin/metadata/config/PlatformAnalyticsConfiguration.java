package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class PlatformAnalyticsConfiguration {
  private boolean enabled;
  private UsageExportConfiguration usageExport;
}
