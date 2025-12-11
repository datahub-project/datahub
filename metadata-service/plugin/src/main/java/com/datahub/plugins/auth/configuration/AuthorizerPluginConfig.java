/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.plugins.auth.configuration;

import com.datahub.plugins.common.PluginType;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Authorizer plugin configuration provided by user. {@link
 * com.datahub.plugins.auth.provider.AuthorizerPluginConfigProvider} instantiate this class
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class AuthorizerPluginConfig extends AuthPluginConfig {
  public AuthorizerPluginConfig(
      String name,
      Boolean enabled,
      String className,
      Path pluginDirectory,
      Path pluginJar,
      Optional<Map<String, Object>> configs) {
    super(PluginType.AUTHORIZER, name, enabled, className, pluginDirectory, pluginJar, configs);
  }
} // currently this class doesn't have any special attributes
