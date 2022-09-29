package com.datahub.plugins.common;

import java.nio.file.Path;
import java.security.ProtectionDomain;


// Common permission manager for authentication and authorization.
public interface PluginPermissionManager {
  ProtectionDomain createProtectionDomain(Path sourceCodeDirectory);
}
