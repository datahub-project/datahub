package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * POJO representing the "ingestion" configuration block in application.yml.
 */
@Data
@AllArgsConstructor
public class IngestionConfiguration {
  /**
   * Whether managed ingestion is enabled
   */
  private boolean enabled;

  /**
   * The default CLI version to use in managed ingestion
   */
  private String defaultCliVersion;
}