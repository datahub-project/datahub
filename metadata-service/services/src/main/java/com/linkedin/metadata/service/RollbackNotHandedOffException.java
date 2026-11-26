package com.linkedin.metadata.service;

import javax.annotation.Nonnull;

/**
 * An {@link IngestionRollbackDispatcher} did not take the rollback and it must not run in this
 * process either, for example because the same run is already rolling back. Nothing was started, so
 * the run's status is left as it is.
 */
public class RollbackNotHandedOffException extends RuntimeException {

  public RollbackNotHandedOffException(@Nonnull final String message) {
    super(message);
  }

  public RollbackNotHandedOffException(
      @Nonnull final String message, @Nonnull final Throwable cause) {
    super(message, cause);
  }
}
