package com.linkedin.metadata.config;

import lombok.Data;

/**
 * POJO representing the mclProcessing configuration block in application.yaml. Contains settings
 * for Metadata Change Log (MCL) processing.
 */
@Data
public class MCLProcessingConfiguration {

  /** CDC (Change Data Capture) source configuration. */
  private CDCSourceConfiguration cdcSource;
}
