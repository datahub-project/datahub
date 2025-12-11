/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.plugins.auth.provider;

import com.datahub.plugins.auth.configuration.AuthParam;
import com.datahub.plugins.auth.configuration.AuthPluginConfig;
import com.datahub.plugins.auth.configuration.AuthorizerPluginConfig;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.YamlMapper;
import com.datahub.plugins.configuration.PluginConfig;
import java.nio.file.Path;
import javax.annotation.Nonnull;

/**
 * Responsible for creating {@link AuthorizerPluginConfig} instance. This provider is register in
 * {@link com.datahub.plugins.factory.PluginConfigFactory} as provider of Authorizer configuration
 */
public class AuthorizerPluginConfigProvider extends AuthPluginConfigProvider {
  @Override
  public PluginType getType() {
    return PluginType.AUTHORIZER;
  }

  @Override
  public AuthPluginConfig createAuthPluginConfig(@Nonnull PluginConfig pluginConfig) {
    // Map Yaml section present in config.yml at plugins[].params to AuthParam
    AuthParam authParam =
        (new YamlMapper<AuthParam>()).fromMap(pluginConfig.getParams(), AuthParam.class);

    // Make the pluginJar file path either from name of plugin or explicitly from
    // plugins[].params.jarFileName
    // This logic is common for authenticator and authorizer plugin and hence define in superclass
    Path pluginJar = formPluginJar(pluginConfig, authParam);

    return new AuthorizerPluginConfig(
        pluginConfig.getName(),
        pluginConfig.getEnabled(),
        authParam.getClassName(),
        pluginConfig.getPluginHomeDirectory(),
        pluginJar,
        authParam.getConfigs());
  }
}
