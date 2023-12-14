package com.datahub.plugins.auth.provider;

import com.datahub.plugins.auth.configuration.AuthParam;
import com.datahub.plugins.auth.configuration.AuthPluginConfig;
import com.datahub.plugins.common.PluginConfigProvider;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.configuration.PluginConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for {@link AuthenticatorPluginConfigProvider} and {@link
 * AuthorizerPluginConfigProvider}.
 */
public abstract class AuthPluginConfigProvider implements PluginConfigProvider<AuthPluginConfig> {
  public abstract PluginType getType();

  public abstract AuthPluginConfig createAuthPluginConfig(PluginConfig pluginConfig);

  @Override
  public List<AuthPluginConfig> processConfig(
      List<com.datahub.plugins.configuration.PluginConfig> pluginConfigConfigs) {
    // Filter out AuthPlugin
    Stream<PluginConfig> authPluginHolder =
        pluginConfigConfigs.stream().filter(pluginHolder -> pluginHolder.getType() == getType());
    // Create AuthPlugin type instances
    List<AuthPluginConfig> authPlugins =
        authPluginHolder.map(this::createAuthPluginConfig).collect(Collectors.toList());
    return authPlugins;
  }

  public Path formPluginJar(PluginConfig pluginConfig, AuthParam authConfig) {
    // User is either going to explicitly set the jarFileName or we will infer it from plugin name
    String jarName = authConfig.getJarFileName().orElse(pluginConfig.getName() + ".jar");
    Path jarPath = Paths.get(pluginConfig.getPluginHomeDirectory().toString(), jarName);
    if (!jarPath.toFile().exists()) {
      throw new IllegalArgumentException(String.format("Plugin Jar %s not found", jarPath));
    }
    return jarPath;
  }
}
