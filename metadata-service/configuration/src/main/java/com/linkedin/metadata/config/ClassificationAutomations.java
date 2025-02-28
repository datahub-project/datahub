package com.linkedin.metadata.config;

import lombok.Data;

/**
 * POJO representing the Classification Automations configuration in classificationConfig in
 * application.yml.on.yml
 */
@Data
public class ClassificationAutomations {
  public boolean snowflake;
  public boolean aiTermClassification;
  // Add other automation flags as needed
}
