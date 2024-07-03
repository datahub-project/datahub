package com.linkedin.metadata.restli;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.errors.ServiceError;

/**
 * Captures an error <i>response</i> (e.g. 404-not-found) that is not to be regarded as an
 * <i>exception</i> within the server. <br>
 * <br>
 * Restli apparently requires http-error-responses to be represented by {@link
 * RestLiServiceException}; thus this class is used to specify an error <i>response</i> that isn't
 * really an <i>exception</i> (in the context of the server).
 */
public class NonExceptionHttpErrorResponse extends RestLiServiceException {

  public NonExceptionHttpErrorResponse(HttpStatus status) {
    super(status);
  }

  public NonExceptionHttpErrorResponse(HttpStatus status, String message) {
    super(status, message);
  }

  public NonExceptionHttpErrorResponse(HttpStatus status, Throwable cause) {
    super(status, cause);
  }

  public NonExceptionHttpErrorResponse(HttpStatus status, String message, Throwable cause) {
    super(status, message, cause);
  }

  public NonExceptionHttpErrorResponse(
      HttpStatus status, String message, Throwable cause, boolean writableStackTrace) {
    super(status, message, cause, writableStackTrace);
  }

  public NonExceptionHttpErrorResponse(ServiceError serviceError) {
    super(serviceError);
  }

  public NonExceptionHttpErrorResponse(ServiceError serviceError, Throwable cause) {
    super(serviceError, cause);
  }
}
