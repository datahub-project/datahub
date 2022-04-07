package com.datahub.authorization;

import java.util.Map;
import javax.annotation.Nonnull;


/**
 * An Authorizer is responsible for determining whether an actor should be granted a specific privilege.
 */
public interface Authorizer {
  /**
   * Initialize the Authorizer. Invoked once at boot time.
   *
   * @param authorizerConfig config provided to the authenticator derived from the Metadata Service YAML config. This
   *                         config comes from the "authorization.authorizers.config" configuration.
   */
  void init(@Nonnull final Map<String, Object> authorizerConfig);

  /**
   * Authorizes an action based on the actor, the resource, & required privileges.
   */
  AuthorizationResult authorize(AuthorizationRequest request);
}
