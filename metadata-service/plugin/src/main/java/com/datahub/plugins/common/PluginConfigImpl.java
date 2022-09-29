package com.datahub.plugins.common;

import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class PluginConfigImpl implements PluginConfig {
  private PluginType type;
  private String name;
  private Boolean enabled;
  private String className;
  private Optional<Map<String, Object>> configs;
}
