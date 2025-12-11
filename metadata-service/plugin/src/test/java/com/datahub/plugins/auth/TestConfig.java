/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
    authorizerConfig.setParams(
        Map.of(
            "className",
            "com.datahub.authorization.ranger.RangerAuthorizer",
            "configs",
            Map.of("username", "foo", "password", "root123")));

    PluginConfig authenticatorConfig = new PluginConfig();
    authorizerConfig.setName("sample-authenticator");
    authorizerConfig.setType(PluginType.AUTHENTICATOR);
    authorizerConfig.setParams(Map.of("className", "com.datahub.plugins.test.TestAuthenticator"));

    List<PluginConfig> plugins = Arrays.asList(authorizerConfig, authenticatorConfig);

    assert Config.builder().plugins(plugins).build() != null;
  }
}
