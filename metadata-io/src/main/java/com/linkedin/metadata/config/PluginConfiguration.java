package com.linkedin.metadata.config;

import lombok.Data;


@Data
public class PluginConfiguration {
  /**
   * Plugin security mode, either RESTRICTED or LENIENT
   */
  // Ideally it should be com.datahub.plugin.common.SecurityMode from metadata-service/plugin, However avoiding to include it as dependency as some modules from metadata-service/ are dependent on metadata-io, so it might create a circular dependency
  private String pluginSecurityMode;

  /**
   * Directory path of entity registry, default to /etc/datahub/plugins/models
   */
  private EntityRegistryPluginConfiguration entityRegistry;
  /**
   * The location where the Retention config files live
   */
  private RetentionPluginConfiguration retention;
  /**
   * Plugin framework's plugin base directory path, default to /etc/datahub/plugins/auth
   */
  private AuthPluginConfiguration auth;
}