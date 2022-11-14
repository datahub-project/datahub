package com.datahub.plugins.auth;

import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.configuration.Config;
import com.datahub.plugins.configuration.ConfigProvider;
import com.datahub.plugins.factory.PluginConfigFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.testng.annotations.Test;


public class TestPluginConfigFactory {

  @Test
  public void authConfig() throws Exception {
    Path pluginBaseDirectory = Paths.get("src", "test", "resources", "valid-base-plugin-dir1");
    ConfigProvider configProvider = new ConfigProvider(pluginBaseDirectory);
    Config config = configProvider.load().orElseThrow(() -> new Exception("Should not be empty"));

    assert config != null;

    PluginConfigFactory authenticatorPluginConfigFactory = new PluginConfigFactory(config);

    // Load authenticator plugin configuration
    List<PluginConfig> authenticatorConfigs =
        authenticatorPluginConfigFactory.loadPluginConfigs(PluginType.AUTHENTICATOR);
    authenticatorConfigs.forEach(c -> {
      assert c.getClassName().equals("com.datahub.ranger.Authenticator"); // className should match to Authenticator
    });

    // Load authorizer plugin configuration
    List<PluginConfig> authorizerConfigs = authenticatorPluginConfigFactory.loadPluginConfigs(PluginType.AUTHORIZER);
    authorizerConfigs.forEach(c -> {
      assert c.getClassName().equals("com.datahub.ranger.Authorizer"); // className should match to Authorizer
    });
  }
}
