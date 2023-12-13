package com.datahub.authentication;

/**
 * A static wrapper around a {@link ThreadLocal} instance of {@link Authentication} containing
 * information about the currently authenticated actor.
 */
public class AuthenticationContext {
  private static final ThreadLocal<Authentication> AUTHENTICATION = new ThreadLocal<>();

  public static Authentication getAuthentication() {
    return AUTHENTICATION.get();
  }

  public static void setAuthentication(Authentication authentication) {
    AUTHENTICATION.set(authentication);
  }

  public static void remove() {
    AUTHENTICATION.remove();
  }

  private AuthenticationContext() {}
}
