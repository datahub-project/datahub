package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class TestsHookExecutionLimitConfiguration {
  private int elasticSearchExecutor;
  private int defaultExecutor;
}
