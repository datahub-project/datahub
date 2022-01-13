package com.datahub.metadata.authentication;

public class AuthenticationContext {
  private static final ThreadLocal<String> ACTOR = new ThreadLocal<>();

  public static String getActor() {
    return ACTOR.get();
  }

  public static void setActor(String actor) {
    ACTOR.set(actor);
  }

  public static void remove() {
    ACTOR.remove();
  }

  private AuthenticationContext() { }
}
