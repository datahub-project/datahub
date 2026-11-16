package com.datahub.authentication.token;

/**
 * The token's signature and claims may be valid, but its revocation state could not be loaded from
 * the store. This is a transient failure, not proof that the token is invalid.
 */
public class TokenStoreUnavailableException extends TokenException {

  public TokenStoreUnavailableException(final String message, final Throwable e) {
    super(message, e);
  }
}
