package com.datahub.plugins.common;

import java.util.Map;
import java.util.Optional;


public interface PluginConfig {
  public PluginType getType();

  public String getName();

  public Boolean getEnabled();

  public String getClassName();

  public Optional<Map<String, Object>> getConfigs();
}
