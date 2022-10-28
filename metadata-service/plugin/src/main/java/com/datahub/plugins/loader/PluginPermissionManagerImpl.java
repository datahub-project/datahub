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


public class PluginPermissionManagerImpl implements PluginPermissionManager {

  private SecurityMode _securityMode;

  public PluginPermissionManagerImpl(SecurityMode securityMode) {
    this._securityMode = securityMode;
  }

  @Override
  public ProtectionDomain createProtectionDomain(Path sourceCodeDirectory) {
    {
      URL url = null;
      try {
        url = sourceCodeDirectory.toUri().toURL();
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
      Permissions permissions = this._securityMode.permissionsSupplier().apply(sourceCodeDirectory);
      CodeSource codeSource = new CodeSource(url, (Certificate[]) null);
      return new ProtectionDomain(codeSource, permissions);
    }
  }
}
