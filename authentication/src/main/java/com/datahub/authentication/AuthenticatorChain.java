package com.datahub.authentication;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * A configurable chain of {@link Authenticator}s executed in series to attempt to authenticate an inbound request.
 *
 * Individual {@link Authenticator}s are registered with the chain using {@link #register(Authenticator)}.
 * The chain can be executed by invoking {@link #authenticate(AuthenticationContext)} with an instance of {@link AuthenticationContext}.
 */
@Slf4j
public class AuthenticatorChain {

  private final Map<String, Object> config;
  private final List<Authenticator> authenticators = new ArrayList<>();

  public AuthenticatorChain(@Nonnull final Map<String, Object> config) {
    Objects.requireNonNull(config);
    this.config = config;
  }

  /**
   * Registers a new {@link Authenticator} at the end of the authentication chain.
   *
   * @param authenticator the authenticator to register
   */
  public void register(@Nonnull final Authenticator authenticator) {
    Objects.requireNonNull(authenticator);
    try {
      authenticator.init(this.config);
      authenticators.add(authenticator);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to register authenticator %s!",
          authenticator.getClass().getCanonicalName()), e);
    }
  }

  /**
   * Executes a set of {@link Authenticator}s and returns the first successful authentication result.
   *
   * Returns an instance of {@link Authentication} if the incoming request is successfully authenticated.
   * Returns null if {@link Authentication} cannot be resolved for the incoming request.
   */
  @Nullable
  public Authentication authenticate(@Nonnull final AuthenticationContext context) {
    Objects.requireNonNull(context);
    for (final Authenticator authenticator : this.authenticators) {
      try {
        Authentication result = authenticator.authenticate(context);
        if (result != null) {
          // Short circuit on success.
          return result;
        }
      } catch (Exception e) {
        log.error(String.format("Failed to authenticate request using Authenticator %s", authenticator.getClass().getCanonicalName()), e);
      }
    }
    // No authentication resolved. Return null.
    return null;
  }
}
