/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package auth;

public class GuestAuthenticationConfigs {
  public static final String GUEST_ENABLED_CONFIG_PATH = "auth.guest.enabled";
  public static final String GUEST_USER_CONFIG_PATH = "auth.guest.user";
  public static final String GUEST_PATH_CONFIG_PATH = "auth.guest.path";
  public static final String DEFAULT_GUEST_USER_NAME = "guest";
  public static final String DEFAULT_GUEST_PATH = "/public";

  private Boolean isEnabled = false;
  private String guestUser =
      DEFAULT_GUEST_USER_NAME; // Default if not defined but guest auth is enabled.
  private String guestPath =
      DEFAULT_GUEST_PATH; // The path for initial access to login as guest and bypass login page.

  public GuestAuthenticationConfigs(final com.typesafe.config.Config configs) {
    if (configs.hasPath(GUEST_ENABLED_CONFIG_PATH)
        && configs.getBoolean(GUEST_ENABLED_CONFIG_PATH)) {
      isEnabled = true;
    }
    if (configs.hasPath(GUEST_USER_CONFIG_PATH)) {
      guestUser = configs.getString(GUEST_USER_CONFIG_PATH);
    }
    if (configs.hasPath(GUEST_PATH_CONFIG_PATH)) {
      guestPath = configs.getString(GUEST_PATH_CONFIG_PATH);
    }
  }

  public boolean isGuestEnabled() {
    return isEnabled;
  }

  public String getGuestUser() {
    return guestUser;
  }

  public String getGuestPath() {
    return guestPath;
  }
}
