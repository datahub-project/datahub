package com.linkedin.metadata.service;

import javax.annotation.Nonnull;

/** Signals that indexing can be safely retried after bounded transient storage retries exhaust. */
public class RetryableIndexingException extends RuntimeException {

  public RetryableIndexingException(@Nonnull String message, @Nonnull Throwable cause) {
    super(message, cause);
  }
}
