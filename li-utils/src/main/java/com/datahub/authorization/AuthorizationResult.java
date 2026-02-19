package com.datahub.authorization;

import lombok.Value;

/** A result returned after requesting authorization for a particular privilege. */
@Value
public class AuthorizationResult {
  /** The original authorization request */
  AuthorizationRequest request;

  /** The result type. Allow or deny the authorization request for the actor. */
  public enum Type {
    /** Allow the request - the requested actor is privileged. */
    ALLOW,
    /** Deny the request - the requested actor is not privileged. */
    DENY
  }

  /** The decision - whether to allow or deny the request. */
  Type type;

  /** Optional message associated with the decision. Useful for debugging. */
  String message;
}
