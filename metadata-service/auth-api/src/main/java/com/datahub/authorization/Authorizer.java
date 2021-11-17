package com.datahub.authorization;

public interface Authorizer {

  enum AuthorizationMode {
    /**
     * Default mode simply means that authorization is enforced, with a DENY result returned
     */
    DEFAULT,
    /**
     * Allow all means that the AuthorizationManager will allow all actions. This is used as an override to disable the
     * policies feature.
     */
    ALLOW_ALL
  }

  /**
   * Authorizes an action based on the actor, the resource, & required privileges.
   */
  AuthorizationResult authorize(AuthorizationRequest request);

  /**
   * Returns the mode the Authorizer is operating in.
   */
  AuthorizationMode mode();
}
