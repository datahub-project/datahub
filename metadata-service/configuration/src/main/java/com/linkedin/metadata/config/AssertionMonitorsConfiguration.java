package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing configurations for how assertion monitors should execute */
@Data
public class AssertionMonitorsConfiguration {
  /**
   * CSV listing aspects that the system should reference to determine an ingestion source that can
   * execute an assertion
   */
  public String resolveIngestionSourceForAspects;
}
