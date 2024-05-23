package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the Classification configuration block in application.yml.on.yml */
@Data
public class ClassificationConfiguration {
  /** Whether the classification is enabled */
  public boolean enabled;

  /** Whether individual automations are enabled */
  public ClassificationAutomations automations;
}
