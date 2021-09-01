package com.datahub.metadata.auth;

public class AuthContext {
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

  private AuthContext() { }
}
