package com.linkedin.metadata.config.telemetry;

import lombok.Data;

@Data
public class GoogleAnalyticsConfiguration {
  public boolean enabled; // Whether Google Analytics is enabled
  public String measurementId; // Google Analytics measurement ID
}
