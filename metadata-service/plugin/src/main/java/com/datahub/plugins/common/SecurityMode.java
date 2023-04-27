package com.datahub.plugins.common;

import java.io.FilePermission;
import java.net.SocketPermission;
import java.nio.file.Path;
import java.security.AllPermission;
import java.security.Permissions;
import java.util.function.Function;


/**
 * Supported security modes
 */
public enum SecurityMode {
  /**
   * In this mode plugins has limited access.
   *
   * Plugins are allowed to connect on below ports only
   *  1) port greater than 1024
   *  2) port 80
   *  3) port 443
   *  All other ports connection are disallowed.
   *
   *  Plugins are allowed to read and write files on PLUGIN_HOME directory only and all other read/write access are
   *  denied.
   */
  RESTRICTED(SecurityMode::restrictModePermissionSupplier),

  /**
   * Plugins has full access.
   * In this mode plugin can read/write to any directory, can connect to any port and can read environment variables.
   */
  LENIENT(SecurityMode::lenientModePermissionSupplier);

  private final Function<Path, Permissions> _permissionsSupplier;

  SecurityMode(Function<Path, Permissions> permissionsSupplier) {
    this._permissionsSupplier = permissionsSupplier;
  }

  private static Permissions restrictModePermissionSupplier(Path sourceCodeDirectory) {
    Permissions permissions = new Permissions();

    permissions.add(new FilePermission(sourceCodeDirectory.toString() + "/*", "read,write,delete"));
    permissions.add(
        new SocketPermission("*:1024-", "connect,resolve")); // Allow to connect access to all socket above 1024
    permissions.add(new SocketPermission("*:80", "connect,resolve")); // Allow to connect access to HTTP port
    permissions.add(new SocketPermission("*:443", "connect,resolve")); // Allow to connect access to HTTPS port

    return permissions;
  }

  private static Permissions lenientModePermissionSupplier(Path sourceCodeDirectory) {
    Permissions permissions = new Permissions();
    permissions.add(new AllPermission());
    return permissions;
  }

  public Function<Path, Permissions> permissionsSupplier() {
    return this._permissionsSupplier;
  }
}
