package com.linkedin.metadata.config;

import lombok.Data;


/**
 * POJO representing visualConfig block in the application.yml.
 */
@Data
public class VisualConfiguration {
  /**
   * Asset related configurations
   */
  public AssetsConfiguration assets;
}
