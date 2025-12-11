/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.datahubusage.event;

import javax.annotation.Nullable;
import lombok.Getter;

public enum LoginSource {
  PASSWORD_RESET("passwordReset"),
  PASSWORD_LOGIN("passwordLogin"),
  FALLBACK_LOGIN("fallbackLogin"),
  SIGN_UP_LINK_LOGIN("signUpLinkLogin"),
  GUEST_LOGIN("guestLogin"),
  SSO_LOGIN("ssoLogin");

  @Getter private final String source;

  LoginSource(String source) {
    this.source = source;
  }

  @Nullable
  public static LoginSource getSource(String name) {
    for (LoginSource loginSource : LoginSource.values()) {
      if (loginSource.source.equalsIgnoreCase(name)) {
        return loginSource;
      }
    }
    return null;
  }
}
