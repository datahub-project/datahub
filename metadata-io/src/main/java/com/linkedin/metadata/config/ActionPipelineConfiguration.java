package com.linkedin.metadata.config;

import lombok.Data;


/**
 * POJO representing the "action" configuration block in application.yml.
 */
@Data
public class ActionPipelineConfiguration {
  /**
   * Whether actions is enabled
   */
  public boolean enabled;

}