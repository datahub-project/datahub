package com.datahub.authentication.authenticator;

import com.datahub.authentication.Authentication;

import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationExpiredException;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.AuthenticatorContext;
import com.linkedin.util.Pair;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * A configurable chain of {@link Authenticator}s executed in series to attempt to authenticate an inbound request.
 *
 * Individual {@link Authenticator}s are registered with the chain using {@link #register(Authenticator)}.
 * The chain can be executed by invoking {@link #authenticate(AuthenticatorContext)} with an instance of {@link AuthenticatorContext}.
 */
@Slf4j
public class AuthenticatorChain {

  private final List<Authenticator> authenticators = new ArrayList<>();

  /**
   * Registers a new {@link Authenticator} at the end of the authentication chain.
   *
   * @param authenticator the authenticator to register
   */
  public void register(@Nonnull final Authenticator authenticator) {
    Objects.requireNonNull(authenticator);
    authenticators.add(authenticator);
  }

  /**
   * Executes a set of {@link Authenticator}s and returns the first successful authentication result.
   *
   * Returns an instance of {@link Authentication} if the incoming request is successfully authenticated.
   * Returns null if {@link Authentication} cannot be resolved for the incoming request.
   */
  @Nullable
  public Authentication authenticate(@Nonnull final AuthenticatorContext context) throws AuthenticationException {
    Objects.requireNonNull(context);
    List<Pair<String, String>> authenticationFailures = new ArrayList<>();
    for (final Authenticator authenticator : this.authenticators) {
      try {
        log.debug(String.format("Executing Authenticator with class name %s", authenticator.getClass().getCanonicalName()));
        Authentication result = authenticator.authenticate(context);
        if (result != null) {
          // Authentication was successful - Short circuit
          return result;
        }
      } catch (AuthenticationExpiredException e) {
        // Throw if it's an AuthenticationException to propagate the error message to the end user
        log.debug(String.format("Unable to authenticate request using Authenticator %s", authenticator.getClass().getCanonicalName()), e);
        throw e;
      } catch (Exception e) {
        // Log as a normal error otherwise.
        authenticationFailures.add(new Pair<>(authenticator.getClass().getCanonicalName(), e.getMessage()));
        log.debug(String.format(
            "Caught exception while attempting to authenticate request using Authenticator %s",
            authenticator.getClass().getCanonicalName()), e);
      }
    }
    // No authentication resolved. Return null.
    if (!authenticationFailures.isEmpty()) {
      log.warn("Authentication chain failed to resolve a valid authentication. Errors: {}", authenticationFailures);
    }
    return null;
  }
}
