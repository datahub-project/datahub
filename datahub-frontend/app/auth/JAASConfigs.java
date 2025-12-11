/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package auth;

/**
 * Currently, this config enables or disable custom Java Authentication and Authorization Service
 * authentication that has traditionally existed in DH.
 */
public class JAASConfigs {

  public static final String JAAS_ENABLED_CONFIG_PATH = "auth.jaas.enabled";

  private Boolean isEnabled = true;

  public JAASConfigs(final com.typesafe.config.Config configs) {
    if (configs.hasPath(JAAS_ENABLED_CONFIG_PATH)
        && !configs.getBoolean(JAAS_ENABLED_CONFIG_PATH)) {
      isEnabled = false;
    }
  }

  public boolean isJAASEnabled() {
    return isEnabled;
  }
}
