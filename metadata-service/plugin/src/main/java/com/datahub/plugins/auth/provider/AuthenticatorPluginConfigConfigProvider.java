package com.datahub.plugins.auth.provider;

import com.datahub.plugins.auth.configuration.AuthConfig;
import com.datahub.plugins.auth.pojo.AuthPluginConfig;
import com.datahub.plugins.auth.pojo.AuthenticatorPluginConfig;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.YamlMapper;
import java.nio.file.Path;


public class AuthenticatorPluginConfigConfigProvider extends AuthPluginConfigProvider {
  @Override
  public PluginType getType() {
    return PluginType.AUTHENTICATOR;
  }

  @Override
  public AuthPluginConfig createAuthPluginConfig(com.datahub.plugins.configuration.PluginConfig pluginConfig) {
    AuthConfig authConfig = (new YamlMapper<AuthConfig>()).fromMap(pluginConfig.getParams(), AuthConfig.class);
    Path pluginJar = formPluginJar(pluginConfig, authConfig);
    return new AuthenticatorPluginConfig(pluginConfig.getName(), pluginConfig.getEnabled(),
        authConfig.getClassName(), pluginConfig.getPluginDirectory(), pluginJar, authConfig.getConfigs());
  }
}

