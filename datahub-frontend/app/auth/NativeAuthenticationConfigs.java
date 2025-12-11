/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package auth;

/** Currently, this config enables or disable native user authentication. */
public class NativeAuthenticationConfigs {

  public static final String NATIVE_AUTHENTICATION_ENABLED_CONFIG_PATH = "auth.native.enabled";
  public static final String NATIVE_AUTHENTICATION_ENFORCE_VALID_EMAIL_ENABLED_CONFIG_PATH =
      "auth.native.signUp.enforceValidEmail";

  private Boolean isEnabled = true;
  private Boolean isEnforceValidEmailEnabled = true;

  public NativeAuthenticationConfigs(final com.typesafe.config.Config configs) {
    if (configs.hasPath(NATIVE_AUTHENTICATION_ENABLED_CONFIG_PATH)) {
      isEnabled =
          Boolean.parseBoolean(
              configs.getValue(NATIVE_AUTHENTICATION_ENABLED_CONFIG_PATH).toString());
    }
    if (configs.hasPath(NATIVE_AUTHENTICATION_ENFORCE_VALID_EMAIL_ENABLED_CONFIG_PATH)) {
      isEnforceValidEmailEnabled =
          Boolean.parseBoolean(
              configs
                  .getValue(NATIVE_AUTHENTICATION_ENFORCE_VALID_EMAIL_ENABLED_CONFIG_PATH)
                  .toString());
    }
  }

  public boolean isNativeAuthenticationEnabled() {
    return isEnabled;
  }

  public boolean isEnforceValidEmailEnabled() {
    return isEnforceValidEmailEnabled;
  }
}
