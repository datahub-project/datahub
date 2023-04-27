package com.datahub.plugins.auth;

import com.datahub.plugins.common.SecurityMode;
import com.datahub.plugins.loader.PluginPermissionManagerImpl;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.ProtectionDomain;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


@Test
public class TestPluginPermissionManager {
  @Test
  public void testRestrictedMode() throws MalformedURLException {
    PluginPermissionManagerImpl pluginPermissionManager = new PluginPermissionManagerImpl(SecurityMode.RESTRICTED);

    Path pluginHome = Paths.get("src", "test", "resources", "valid-base-plugin-dir1", "apache-ranger-authenticator");

    ProtectionDomain protectionDomain = pluginPermissionManager.createProtectionDomain(pluginHome.toAbsolutePath());

    // provided pluginHome and codeSource in protection domain should be equal
    assert pluginHome.toUri()
        .toURL()
        .toExternalForm()
        .equals(protectionDomain.getCodeSource().getLocation().toExternalForm());

    PermissionCollection permissionCollection = protectionDomain.getPermissions();
    List<Permission> permissions = Collections.list(permissionCollection.elements());
    // It should have 4 permissions
    assert permissions.size() == 4;

    Map<String, String> map = new HashMap<>(); // expected permissions
    map.put("*:1024-", "connect,resolve");
    map.put("*:80", "connect,resolve");
    map.put("*:443", "connect,resolve");
    map.put(pluginHome.toAbsolutePath() + "/*", "read,write,delete");

    // Compare actual with expected
    permissions.forEach(permission -> {
      assert map.keySet().contains(permission.getName());
      assert map.values().contains(permission.getActions());
    });
  }

  public void testLenientMode() throws MalformedURLException {
    PluginPermissionManagerImpl pluginPermissionManager = new PluginPermissionManagerImpl(SecurityMode.LENIENT);

    Path pluginHome = Paths.get("src", "test", "resources", "valid-base-plugin-dir1", "apache-ranger-authenticator");

    ProtectionDomain protectionDomain = pluginPermissionManager.createProtectionDomain(pluginHome.toAbsolutePath());

    // provided pluginHome and codeSource in protection domain should be equal
    assert pluginHome.toUri()
        .toURL()
        .toExternalForm()
        .equals(protectionDomain.getCodeSource().getLocation().toExternalForm());

    PermissionCollection permissionCollection = protectionDomain.getPermissions();
    List<Permission> permissions = Collections.list(permissionCollection.elements());

    // It should have 1 permission
    assert permissions.size() == 1;

    permissions.forEach(permission -> {
      assert permission.getName().equals("<all permissions>");
    });
  }
}
