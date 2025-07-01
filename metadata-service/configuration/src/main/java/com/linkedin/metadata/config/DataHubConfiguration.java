package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "datahub" configuration block in application.yaml. */
@Data
public class DataHubConfiguration {
  /**
   * Indicates the type of server that has been deployed: quickstart, prod, or a custom
   * configuration
   */
  public String serverType;

  public String serverEnv;

  private PluginConfiguration plugin;
}
