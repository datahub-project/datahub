package com.datahub.authentication;

import java.util.Map;


public class ConfigProvider {

  private final Map<String, Object> configs;

  public ConfigProvider(final Map<String, Object> configs) {
    this.configs = configs;
  }

  public Object getConfig(final String configPath) {
    return this.configs.get(configPath);
  }

  public Object getConfigOrDefault(final String configPath, final Object def) {
    return this.configs.getOrDefault(configPath, def);
  }
}
