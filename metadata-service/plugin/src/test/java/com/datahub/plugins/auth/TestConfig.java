package com.datahub.plugins.auth;

import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.configuration.Config;
import com.datahub.plugins.configuration.PluginConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


@Test
public class TestConfig {
  @Test
  public void testConfig() {
    PluginConfig authorizerConfig = new PluginConfig();
    authorizerConfig.setName("apache-ranger-authorizer");
    authorizerConfig.setType(PluginType.AUTHORIZER);
    authorizerConfig.setParams(Map.of("className", "com.datahub.authorization.ranger.RangerAuthorizer", "configs",
        Map.of("username", "foo", "password", "root123")));

    PluginConfig authenticatorConfig = new PluginConfig();
    authorizerConfig.setName("sample-authenticator");
    authorizerConfig.setType(PluginType.AUTHENTICATOR);
    authorizerConfig.setParams(Map.of("className", "com.datahub.plugins.test.TestAuthenticator"));

    List<PluginConfig> plugins = Arrays.asList(authorizerConfig, authenticatorConfig);

    assert Config.builder().plugins(plugins).build() != null;
  }
}
