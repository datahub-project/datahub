package com.linkedin.metadata.config;

import lombok.Data;


@Data
public class PluginConfiguration {
  /* Ideally it should be com.datahub.plugin.common.SecurityMode from metadata-service/plugin, However avoiding to include it as dependency
  As some modules from metadata-service/ are dependent on metadata-io, so it might create a circular dependency
   */
  private String pluginSecurityMode;
  private EntityRegistryPluginConfiguration entityRegistry;
  private RetentionPluginConfiguration retention;
  private AuthPluginConfiguration auth;
}