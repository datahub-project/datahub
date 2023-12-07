package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "tests" configuration block in application.yml.on.yml */
@Data
public class TestsConfiguration {
  /** Whether tests are enabled */
  public boolean enabled;
}
