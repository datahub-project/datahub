package com.datahub.authentication.token;

/** A checked exception that is thrown when a DataHub-issued access token cannot be verified. */
public class TokenExpiredException extends TokenException {

  public TokenExpiredException(final String message) {
    super(message);
  }

  public TokenExpiredException(final String message, final Throwable e) {
    super(message, e);
  }
}
