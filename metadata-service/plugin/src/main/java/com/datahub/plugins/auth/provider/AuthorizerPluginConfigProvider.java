package com.datahub.plugins.auth.provider;

import com.datahub.plugins.auth.configuration.AuthParam;
import com.datahub.plugins.auth.configuration.AuthPluginConfig;
import com.datahub.plugins.auth.configuration.AuthorizerPluginConfig;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.YamlMapper;
import com.datahub.plugins.configuration.PluginConfig;
import java.nio.file.Path;


public class AuthorizerPluginConfigProvider extends AuthPluginConfigProvider {
  @Override
  public PluginType getType() {
    return PluginType.AUTHORIZER;
  }

  @Override
  public AuthPluginConfig createAuthPluginConfig(PluginConfig pluginConfig) {
    AuthParam authParam = (new YamlMapper<AuthParam>()).fromMap(pluginConfig.getParams(), AuthParam.class);
    Path pluginJar = formPluginJar(pluginConfig, authParam);
    return new AuthorizerPluginConfig(pluginConfig.getName(), pluginConfig.getEnabled(), authParam.getClassName(),
        pluginConfig.getPluginDirectory(), pluginJar, authParam.getConfigs());
  }
}
