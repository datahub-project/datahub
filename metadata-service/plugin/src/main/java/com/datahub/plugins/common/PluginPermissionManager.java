package com.datahub.plugins.common;

import java.nio.file.Path;
import java.security.ProtectionDomain;


/**
 * Implement this interface to create Java SecurityManager's ProtectionDomain for the plugin.
 */
public interface PluginPermissionManager {
  /**
   * Create codeSource instance for the location of pluginHome to apply SecurityMode restriction to the plugin code
   * @param pluginHome
   * @return ProtectionDomain
   */
  ProtectionDomain createProtectionDomain(Path pluginHome);
}
