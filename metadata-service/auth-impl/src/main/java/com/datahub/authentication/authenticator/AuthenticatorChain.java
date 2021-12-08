package com.datahub.authentication.authenticator;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.AuthenticatorContext;
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
  public Authentication authenticate(@Nonnull final AuthenticatorContext context) {
    Objects.requireNonNull(context);
    for (final Authenticator authenticator : this.authenticators) {
      try {
        log.debug(String.format("Executing Authenticator with class name %s", authenticator.getClass().getCanonicalName()));
        Authentication result = authenticator.authenticate(context);
        if (result != null) {
          // Authentication was successful - Short circuit
          return result;
        }
      } catch (AuthenticationException e) {
        // Simply log and continue if it's an AuthenticationException.
        log.debug(String.format("Unable to authenticate request using Authenticator %s", authenticator.getClass().getCanonicalName()), e);
      } catch (Exception e) {
        // Log as a normal error otherwise.
        log.error(String.format(
            "Caught exception while attempting to authenticate request using Authenticator %s",
            authenticator.getClass().getCanonicalName()), e);
      }
    }
    // No authentication resolved. Return null.
    return null;
  }
}
