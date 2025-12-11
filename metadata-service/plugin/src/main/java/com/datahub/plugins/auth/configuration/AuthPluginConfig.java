/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.plugins.auth.configuration;

import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginType;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/** Superclass for {@link AuthenticatorPluginConfig} and {@link AuthorizerPluginConfig} */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class AuthPluginConfig extends PluginConfig {
  public AuthPluginConfig(
      PluginType type,
      String name,
      Boolean enabled,
      String className,
      Path pluginHomeDirectory,
      Path pluginJarPath,
      Optional<Map<String, Object>> configs) {
    super(type, name, enabled, className, pluginHomeDirectory, pluginJarPath, configs);
  }
}
