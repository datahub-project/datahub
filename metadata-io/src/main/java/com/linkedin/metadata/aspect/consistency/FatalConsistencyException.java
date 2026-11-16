package com.linkedin.metadata.aspect.consistency;

/**
 * A consistency check failed in a way that must stop the batch.
 *
 * <p>The service rethrows this type. Other exceptions from a check are logged and the remaining
 * checks still run. A swallowed failure would publish the next cursor and skip the page.
 */
public class FatalConsistencyException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public FatalConsistencyException(String message) {
    super(message);
  }

  public FatalConsistencyException(String message, Throwable cause) {
    super(message, cause);
  }
}
