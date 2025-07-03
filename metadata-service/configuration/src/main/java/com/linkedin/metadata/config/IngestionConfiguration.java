package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "ingestion" configuration block in application.yaml. */
@Data
public class IngestionConfiguration {
  /** Whether managed ingestion is enabled */
  private boolean enabled;

  /** The default CLI version to use in managed ingestion */
  private String defaultCliVersion;

  private Integer batchRefreshCount;
}
