package com.datahub.plugins.common;

import java.io.FilePermission;
import java.net.SocketPermission;
import java.nio.file.Path;
import java.security.AllPermission;
import java.security.Permissions;
import java.util.function.Function;


public enum SecurityMode {
  RESTRICTED(SecurityMode::restrictModePermissionSupplier), LENIENT(SecurityMode::lenientModePermissionSupplier);

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
