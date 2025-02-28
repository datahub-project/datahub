package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class TestsHookConfiguration {
  private boolean enabled;
  private boolean onTestChange;
  private TestsHookExecutionLimitConfiguration hookExecutionLimit;
  private boolean onEntityChange;
}
