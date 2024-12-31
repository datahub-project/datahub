package com.datahub.authentication.authenticator;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationExpiredException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.plugins.auth.authentication.Authenticator;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A configurable chain of {@link Authenticator}s executed in series to attempt to authenticate an
 * inbound request.
 *
 * <p>Individual {@link Authenticator}s are registered with the chain using {@link
 * #register(Authenticator)}. The chain can be executed by invoking {@link
 * #authenticate(AuthenticationRequest)} with an instance of {@link AuthenticationRequest}.
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
   * Executes a set of {@link Authenticator}s and returns the first successful authentication
   * result.
   *
   * <p>Returns an instance of {@link Authentication} if the incoming request is successfully
   * authenticated. Returns null if {@link Authentication} cannot be resolved for the incoming
   * request.
   */
  @Nullable
  public Authentication authenticate(
      @Nonnull final AuthenticationRequest context, boolean logExceptions)
      throws AuthenticationException {
    Objects.requireNonNull(context);
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    List<Pair<String, Exception>> authenticationFailures = new ArrayList<>();
    for (final Authenticator authenticator : this.authenticators) {
      try {
        log.debug(
            String.format(
                "Executing Authenticator with class name %s",
                authenticator.getClass().getCanonicalName()));
        // The library came with plugin can use the contextClassLoader to load the classes. For
        // example apache-ranger library does this.
        // Here we need to set our IsolatedClassLoader as contextClassLoader to resolve such class
        // loading request from plugin's home directory,
        // otherwise plugin's internal library wouldn't be able to find their dependent classes
        Thread.currentThread().setContextClassLoader(authenticator.getClass().getClassLoader());
        Authentication result = authenticator.authenticate(context);
        // reset
        Thread.currentThread().setContextClassLoader(contextClassLoader);
        if (result != null) {
          // Authentication was successful - Short circuit
          return result;
        }
      } catch (AuthenticationExpiredException e) {
        // Throw if it's an AuthenticationException to propagate the error message to the end user
        log.debug(
            String.format(
                "Unable to authenticate request using Authenticator %s",
                authenticator.getClass().getCanonicalName()),
            e);
        throw e;
      } catch (Exception e) {
        // Log as a normal error otherwise.
        log.debug(
            String.format(
                "Caught exception while attempting to authenticate request using Authenticator %s",
                authenticator.getClass().getCanonicalName()),
            e);
        authenticationFailures.add(new Pair<>(authenticator.getClass().getCanonicalName(), e));
      } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
    }
    // No authentication resolved. Return null.
    if (!authenticationFailures.isEmpty()) {
      List<Pair<String, String>> shortMessage =
          authenticationFailures.stream()
              .peek(
                  p -> {
                    if (logExceptions) {
                      log.error("Error during {} authentication: ", p.getFirst(), p.getSecond());
                    }
                  })
              .map(p -> Pair.of(p.getFirst(), p.getSecond().getMessage()))
              .collect(Collectors.toList());
      log.warn(
          "Authentication chain failed to resolve a valid authentication. Errors: {}",
          shortMessage);
    }
    return null;
  }
}
