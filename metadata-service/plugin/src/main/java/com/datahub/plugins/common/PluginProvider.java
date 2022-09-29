package com.datahub.plugins.common;

import java.util.List;


public interface PluginProvider<T extends PluginConfig> {
  public List<T> processConfig(List<com.datahub.plugins.configuration.PluginConfig> pluginConfigConfigs);
}
