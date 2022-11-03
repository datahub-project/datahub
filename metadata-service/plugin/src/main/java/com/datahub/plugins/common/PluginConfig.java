package com.datahub.plugins.common;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


// Flat form of plugin config
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PluginConfig {
  private PluginType type;
  private String name;
  private Boolean enabled;
  private String className;
  private Path pluginHomeDirectory;
  private Path pluginJarPath;

  private Optional<Map<String, Object>> configs;
}
