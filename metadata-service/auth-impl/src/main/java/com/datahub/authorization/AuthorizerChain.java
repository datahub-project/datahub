package com.datahub.authorization;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * A configurable chain of {@link Authorizer}s executed in series to attempt to authenticate an inbound request.
 *
 * Individual {@link Authorizer}s are registered with the chain using {@link #register(Authorizer)}.
 * The chain can be executed by invoking {@link #authorize(AuthorizationRequest)}.
 */
@Slf4j
public class AuthorizerChain implements Authorizer {

  private final List<Authorizer> authorizers;

  public AuthorizerChain(final List<Authorizer> authorizers) {
    this.authorizers = Objects.requireNonNull(authorizers);
  }

  @Override
  public void init(@Nonnull Map<String, Object> authorizerConfig) {
    // pass.
  }

  /**
   * Executes a set of {@link Authorizer}s and returns the first successful authentication result.
   *
   * Returns an instance of {@link AuthorizationResult}.
   */
  @Nullable
  public AuthorizationResult authorize(@Nonnull final AuthorizationRequest request) {
    Objects.requireNonNull(request);
    for (final Authorizer authorizer : this.authorizers) {
      try {
        log.debug(String.format("Executing Authorizer with class name %s", authorizer.getClass().getCanonicalName()));
        AuthorizationResult result = authorizer.authorize(request);
        if (AuthorizationResult.Type.ALLOW.equals(result.type)) {
          // Authorization was successful - Short circuit
          return result;
        } else {
          log.debug("Received DENY result from Authorizer with class name {}. message: {}", authorizer.getClass().getCanonicalName(), result.getMessage());
        }
      } catch (Exception e) {
        log.debug(String.format(
            "Caught exception while attempting to authorize request using Authorizer %s. Skipping authorizer.",
            authorizer.getClass().getCanonicalName()), e);
      }
    }
    // Return failed Authorization result.
    return new AuthorizationResult(request, AuthorizationResult.Type.DENY, null);
  }

  /**
   * Returns an instance of {@link DataHubAuthorizer} if it is present in the Authentication chain,
   * or null if it cannot be found.
   */
  public DataHubAuthorizer getDefaultAuthorizer() {
    return (DataHubAuthorizer) this.authorizers.stream()
        .filter(authorizer -> authorizer instanceof DataHubAuthorizer)
        .findFirst()
        .orElse(null);
  }
}