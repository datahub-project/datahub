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
