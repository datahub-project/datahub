package com.datahub.authentication;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;


/**
 * An {@link Authenticator}'s job is to authenticate an inbound request by resolving the provided {@link AuthenticationRequest}
 * to an instance of {@link Authentication}, which includes an authenticated {@link Actor} within.
 *
 * In the case that {@link Authentication} cannot be resolved, for example because the request is missing the required
 * authentication information, an {@link AuthenticationException} may be thrown.
 */
public interface Authenticator {

  /**
   * Initialize the Authenticator. Invoked once at boot time.
   *
   * @param authenticatorConfig config provided to the authenticator derived from the Metadata Service YAML config. This
   *                            config comes from the "authentication.authenticators.config" configuration.
   * @param context             nullable configuration objects that are potentially required by an Authenticator instance.
   */
  void init(@Nonnull final Map<String, Object> authenticatorConfig, @Nullable final AuthenticatorContext context);

  /**
   * Authenticates an inbound request given an instance of the {@link AuthenticationRequest}.
   *
   * If the request is authenticated successfully, an instance of {@link Authentication} is returned.
   * If the request cannot be authenticated, returns "null" or throws an {@link AuthenticationException}.
   */
  @Nullable
  Authentication authenticate(@Nonnull final AuthenticationRequest context) throws AuthenticationException;
}
