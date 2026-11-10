package com.datahub.authentication.invite;

/**
 * Thrown when a caller may manage user credentials but may not mint or read an invite token that
 * carries a role. GraphQL resolvers translate this to an authorization error.
 */
public class InviteTokenAuthorizationException extends RuntimeException {
  public InviteTokenAuthorizationException(String message) {
    super(message);
  }
}
