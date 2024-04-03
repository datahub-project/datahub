package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "views" configuration block in application.yml.on.yml */
@Data
public class ViewsConfiguration {
  /** Whether Views are enabled */
  public boolean enabled;
}
