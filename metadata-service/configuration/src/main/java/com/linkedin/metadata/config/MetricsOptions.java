package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class MetricsOptions {
  private String percentiles;
  private String slo;
  private long maxExpectedValue;
}
