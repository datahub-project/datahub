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
   * Returns the current authentication when both the ThreadLocal value and its actor are present.
   * Prefer this at HTTP entrances before {@code auth.getActor().toUrnStr()} to avoid NPEs when the
   * filter left context unset (excluded paths, tests, filter-order bugs).
   */
  @Nonnull
  public static Optional<Authentication> maybeAuthentication() {
    Authentication authentication = AUTHENTICATION.get();
    if (authentication == null || authentication.getActor() == null) {
      return Optional.empty();
    }
    return Optional.of(authentication);
  }

  /** Actor URN for the current request when authentication is present. */
  @Nonnull
  public static Optional<String> maybeActorUrn() {
    return maybeAuthentication().map(auth -> auth.getActor().toUrnStr());
  }

  public static void setAuthentication(Authentication authentication) {
    AUTHENTICATION.set(authentication);
  }

  public static void remove() {
    AUTHENTICATION.remove();
  }

  private AuthenticationContext() {}
}
