package com.datahub.plugins.factory;

import com.datahub.plugins.auth.provider.AuthenticatorPluginConfigProvider;
import com.datahub.plugins.auth.provider.AuthorizerPluginConfigProvider;
import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginProvider;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.configuration.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Return instance of config provider as per type mentioned in {@link Config}
 * @param <T>
 *
 */
public class PluginConfigFactory<T extends PluginConfig> {
  private final static Map<PluginType, PluginProvider> CONFIG_PROVIDER_REGISTRY;

  static {
    CONFIG_PROVIDER_REGISTRY = new HashMap<>(2);
    CONFIG_PROVIDER_REGISTRY.put(PluginType.AUTHENTICATOR, new AuthenticatorPluginConfigProvider());
    CONFIG_PROVIDER_REGISTRY.put(PluginType.AUTHORIZER, new AuthorizerPluginConfigProvider());
  }

  private final Config _config;

  public PluginConfigFactory(@Nonnull Config config) {
    this._config = config;
  }

  @Nonnull
  public List<T> loadPluginConfigs(@Nonnull PluginType pluginType) {
    return CONFIG_PROVIDER_REGISTRY.get(pluginType).processConfig(this._config.getPlugins());
  }
}
