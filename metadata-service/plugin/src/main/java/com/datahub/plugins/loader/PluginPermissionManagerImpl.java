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
