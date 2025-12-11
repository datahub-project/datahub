/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.plugins.loader;

import com.datahub.plugins.common.PluginPermissionManager;
import com.datahub.plugins.common.SecurityMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import javax.annotation.Nonnull;

public class PluginPermissionManagerImpl implements PluginPermissionManager {

  private final SecurityMode _securityMode;

  public PluginPermissionManagerImpl(@Nonnull SecurityMode securityMode) {
    this._securityMode = securityMode;
  }

  /**
   * Create codeSource instance for the location of pluginHome to apply SecurityMode restriction to
   * the plugin code
   *
   * @param pluginHome
   * @return ProtectionDomain
   */
  @Override
  public ProtectionDomain createProtectionDomain(@Nonnull Path pluginHome) {
    {
      URL url = null;
      try {
        url = pluginHome.toUri().toURL();
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
      Permissions permissions = this._securityMode.permissionsSupplier().apply(pluginHome);
      CodeSource codeSource = new CodeSource(url, (Certificate[]) null);
      return new ProtectionDomain(codeSource, permissions);
    }
  }
}
