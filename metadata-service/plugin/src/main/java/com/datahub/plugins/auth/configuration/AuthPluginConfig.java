package com.datahub.plugins.auth.configuration;

import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginType;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class AuthPluginConfig extends PluginConfig {
  // No extra parameter needed in this class, The PluginConfig provided by framework is sufficient for processing.
  public AuthPluginConfig(PluginType type, String name, Boolean enabled, String className, Path pluginHomeDirectory,
      Path pluginJarPath, Optional<Map<String, Object>> configs) {
    super(type, name, enabled, className, pluginHomeDirectory, pluginJarPath, configs);
  }
}
