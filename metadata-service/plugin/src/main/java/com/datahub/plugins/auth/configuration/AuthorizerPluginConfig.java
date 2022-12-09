package com.datahub.plugins.auth.configuration;

import com.datahub.plugins.common.PluginType;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;


/**
 * Authorizer plugin configuration provided by user.
 * {@link com.datahub.plugins.auth.provider.AuthorizerPluginConfigProvider} instantiate this class
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class AuthorizerPluginConfig extends AuthPluginConfig {
  public AuthorizerPluginConfig(String name, Boolean enabled, String className, Path pluginDirectory, Path pluginJar,
      Optional<Map<String, Object>> configs) {
    super(PluginType.AUTHORIZER, name, enabled, className, pluginDirectory, pluginJar, configs);
  }
} // currently this class doesn't have any special attributes
