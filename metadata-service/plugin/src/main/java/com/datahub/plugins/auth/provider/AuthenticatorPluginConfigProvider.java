package com.datahub.plugins.auth.provider;

import com.datahub.plugins.auth.configuration.AuthParam;
import com.datahub.plugins.auth.configuration.AuthPluginConfig;
import com.datahub.plugins.auth.configuration.AuthenticatorPluginConfig;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.YamlMapper;
import java.nio.file.Path;


public class AuthenticatorPluginConfigProvider extends AuthPluginConfigProvider {
  @Override
  public PluginType getType() {
    return PluginType.AUTHENTICATOR;
  }

  @Override
  public AuthPluginConfig createAuthPluginConfig(com.datahub.plugins.configuration.PluginConfig pluginConfig) {
    AuthParam authParam = (new YamlMapper<AuthParam>()).fromMap(pluginConfig.getParams(), AuthParam.class);
    Path pluginJar = formPluginJar(pluginConfig, authParam);
    return new AuthenticatorPluginConfig(pluginConfig.getName(), pluginConfig.getEnabled(), authParam.getClassName(),
        pluginConfig.getPluginDirectory(), pluginJar, authParam.getConfigs());
  }
}

