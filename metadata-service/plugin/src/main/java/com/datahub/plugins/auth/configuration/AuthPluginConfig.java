package com.datahub.plugins.auth.configuration;

import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginType;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;


/**
 * Superclass for {@link AuthenticatorPluginConfig} and {@link AuthorizerPluginConfig}
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class AuthPluginConfig extends PluginConfig {
  public AuthPluginConfig(PluginType type, String name, Boolean enabled, String className, Path pluginHomeDirectory,
      Path pluginJarPath, Optional<Map<String, Object>> configs) {
    super(type, name, enabled, className, pluginHomeDirectory, pluginJarPath, configs);
  }
}
