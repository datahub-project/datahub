package com.datahub.metadata.authentication;

public class AuthenticationContext {
  private static final ThreadLocal<String> PRINCIPAL = new ThreadLocal<String>();

  public static String getPrincipal() {
    return PRINCIPAL.get();
  }

  public static void setPrincipal(String principal) {
    PRINCIPAL.set(principal);
  }

  public static void remove() {
    PRINCIPAL.remove();
  }

  private AuthenticationContext() { }
}
