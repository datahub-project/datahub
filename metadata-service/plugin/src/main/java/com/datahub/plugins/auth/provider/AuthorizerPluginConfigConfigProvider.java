package com.datahub.plugins.auth.provider;

import com.datahub.plugins.auth.configuration.AuthConfig;
import com.datahub.plugins.auth.pojo.AuthPluginConfig;
import com.datahub.plugins.auth.pojo.AuthorizerPluginConfig;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.YamlMapper;
import com.datahub.plugins.configuration.PluginConfig;
import java.nio.file.Path;


public class AuthorizerPluginConfigConfigProvider extends AuthPluginConfigProvider {
  @Override
  public PluginType getType() {
    return PluginType.AUTHORIZER;
  }

  @Override
  public AuthPluginConfig createAuthPluginConfig(PluginConfig pluginConfig) {
    AuthConfig authConfig = (new YamlMapper<AuthConfig>()).fromMap(pluginConfig.getParams(), AuthConfig.class);
    Path pluginJar = formPluginJar(pluginConfig, authConfig);
    return new AuthorizerPluginConfig(pluginConfig.getName(), pluginConfig.getEnabled(),
        authConfig.getClassName(), pluginConfig.getPluginDirectory(), pluginJar, authConfig.getConfigs());
  }
}
