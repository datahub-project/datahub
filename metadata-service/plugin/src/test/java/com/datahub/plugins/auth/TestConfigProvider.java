/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.plugins.auth;

import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.configuration.Config;
import com.datahub.plugins.configuration.ConfigProvider;
import com.datahub.plugins.factory.PluginConfigFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import org.testng.annotations.Test;

@Test
public class TestConfigProvider {
  @Test
  public void testConfigurationLoading() throws Exception {
    Path pluginBaseDirectory = Paths.get("src", "test", "resources", "valid-base-plugin-dir1");
    ConfigProvider configProvider = new ConfigProvider(pluginBaseDirectory);
    Config config = configProvider.load().orElseThrow(() -> new Exception("Should not be empty"));

    assert config != null;

    PluginConfigFactory authenticatorPluginPluginConfigFactory = new PluginConfigFactory(config);
    List<PluginConfig> authenticators =
        authenticatorPluginPluginConfigFactory.loadPluginConfigs(PluginType.AUTHENTICATOR);

    List<PluginConfig> authorizers =
        authenticatorPluginPluginConfigFactory.loadPluginConfigs(PluginType.AUTHORIZER);

    assert authenticators.size() != 0;
    assert authorizers.size() != 0;

    Consumer<PluginConfig> validateAuthenticationPlugin =
        (plugin) -> {
          assert plugin.getName().equals("apache-ranger-authenticator");

          assert "com.datahub.ranger.Authenticator".equals(plugin.getClassName());

          assert plugin.getEnabled();

          String pluginJarPath =
              Paths.get(
                      pluginBaseDirectory.toString(),
                      "apache-ranger-authenticator",
                      "apache-ranger-authenticator.jar")
                  .toAbsolutePath()
                  .toString();
          assert pluginJarPath.equals(plugin.getPluginJarPath().toString());

          String pluginDirectory =
              Paths.get(pluginBaseDirectory.toString(), plugin.getName())
                  .toAbsolutePath()
                  .toString();
          assert pluginDirectory.equals(plugin.getPluginHomeDirectory().toString());
        };

    Consumer<PluginConfig> validateAuthorizationPlugin =
        (plugin) -> {
          assert plugin.getName().equals("apache-ranger-authorizer");

          assert "com.datahub.ranger.Authorizer".equals(plugin.getClassName());

          assert plugin.getEnabled();

          assert Paths.get(
                  pluginBaseDirectory.toString(),
                  "apache-ranger-authorizer",
                  "apache-ranger-authorizer.jar")
              .toAbsolutePath()
              .toString()
              .equals(plugin.getPluginJarPath().toString());

          assert Paths.get(pluginBaseDirectory.toString(), plugin.getName())
              .toAbsolutePath()
              .toString()
              .equals(plugin.getPluginHomeDirectory().toString());
        };

    authenticators.forEach(validateAuthenticationPlugin);
    authorizers.forEach(validateAuthorizationPlugin);
  }
}
