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

  /**
   * Resolve a login source from either the camelCase wire value (e.g. {@code passwordLogin}) or the
   * enum constant name (e.g. {@code PASSWORD_LOGIN}). Matching is case-insensitive.
   */
  @Nullable
  public static LoginSource getSource(@Nullable final String name) {
    if (name == null || name.isBlank()) {
      return null;
    }
    for (LoginSource loginSource : LoginSource.values()) {
      if (loginSource.source.equalsIgnoreCase(name) || loginSource.name().equalsIgnoreCase(name)) {
        return loginSource;
      }
    }
    return null;
  }
}
