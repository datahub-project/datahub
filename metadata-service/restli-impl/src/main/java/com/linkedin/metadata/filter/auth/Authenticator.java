package com.linkedin.metadata.filter.auth;

public interface Authenticator {

  /**
   * Method responsible for authenticating a principal.
   * Throws {@link AuthenticationError} if a valid principal cannot be authenticated.
   */
  AuthenticationResult authenticate(final AuthenticationContext ctx);

}
