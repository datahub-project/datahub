package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class TestsHookConfiguration {
  private boolean enabled;
  private boolean onTestChange;
  private TestsHookExecutionLimitConfiguration hookExecutionLimit;
  private boolean onEntityChange;
}
