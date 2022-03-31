package com.linkedin.metadata.config;

import lombok.Data;

/**
 * POJO representing the "ingestion" configuration block in application.yml.
 */
@Data
public class IngestionConfiguration {
  /**
   * Whether managed ingestion is enabled
   */
  public boolean enabled;

  /**
   * The default CLI version to use in managed ingestion
   */
  public String defaultCliVersion;
}