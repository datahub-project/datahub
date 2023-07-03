package com.datahub.plugins.common;

import java.util.List;


public interface PluginConfigProvider<T extends PluginConfig> {
  List<T> processConfig(List<com.datahub.plugins.configuration.PluginConfig> pluginConfigConfigs);
}
