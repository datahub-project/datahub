package com.datahub.authentication;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A static wrapper around a {@link ThreadLocal} instance of {@link Authentication} containing
 * information about the currently authenticated actor.
 */
public class AuthenticationContext {
  private static final ThreadLocal<Authentication> AUTHENTICATION = new ThreadLocal<>();

  @Nullable
  public static Authentication getAuthentication() {
    return AUTHENTICATION.get();
  }

  /**
   * Present when AuthenticationContext is set for the current thread. Actor is non-null by {@link
   * Authentication} construction ({@code Objects.requireNonNull}).
   */
  @Nonnull
  public static Optional<Authentication> maybeAuthentication() {
    return Optional.ofNullable(AUTHENTICATION.get());
  }

  public static void setAuthentication(Authentication authentication) {
    AUTHENTICATION.set(authentication);
  }

  public static void remove() {
    AUTHENTICATION.remove();
  }

  private AuthenticationContext() {}
}
