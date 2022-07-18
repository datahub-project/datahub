package com.linkedin.metadata.config;

import lombok.Data;
/**
 * POJO representing the "datahub" configuration block in application.yml.
 */
@Data
public class DatahubConfiguration {
  /**
   * Indicates the type of server that has been deployed: quickstart, prod, or a custom configuration
   */
  public String serverType;
}