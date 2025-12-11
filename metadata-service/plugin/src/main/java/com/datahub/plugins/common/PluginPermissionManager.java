/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.plugins.common;

import java.nio.file.Path;
import java.security.ProtectionDomain;

/** Implement this interface to create Java SecurityManager's ProtectionDomain for the plugin. */
public interface PluginPermissionManager {
  /**
   * Create codeSource instance for the location of pluginHome to apply SecurityMode restriction to
   * the plugin code
   *
   * @param pluginHome
   * @return ProtectionDomain
   */
  ProtectionDomain createProtectionDomain(Path pluginHome);
}
