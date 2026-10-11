package com.linkedin.metadata.search.utils;

/**
 * Thrown when a search hit cannot be turned into a result because its {@code urn} field is missing
 * or malformed (e.g. documents created by older bootstrap code).
 *
 * <p>This is a recoverable, data-quality failure: callers can safely skip the offending hit and
 * continue. It exists as a dedicated type so that those callers can catch <em>only</em> this
 * condition and let genuinely unexpected failures propagate instead of silently dropping results.
 *
 * @see UrnExtractionUtils#extractUrnFromSearchHit
 */
public class InvalidSearchHitException extends RuntimeException {

  public InvalidSearchHitException(String message) {
    super(message);
  }

  public InvalidSearchHitException(String message, Throwable cause) {
    super(message, cause);
  }
}
