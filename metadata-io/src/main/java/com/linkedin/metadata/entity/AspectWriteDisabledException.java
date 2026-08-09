package com.linkedin.metadata.entity;

import javax.annotation.Nonnull;

/**
 * Thrown when an aspect write is attempted while the {@link AspectDao} is in read-only mode ({@code
 * canWrite=false}).
 *
 * <p>This is <b>not</b> a {@link jakarta.persistence.PersistenceException}, so {@link
 * AspectDao#runInTransactionWithRetry} must not treat it as a transient conflict and retry. Under
 * optimistic locking, returning an empty conditional-update result would otherwise look like {@link
 * ConditionalWriteOutcome#CONFLICT} and burn retries until {@link
 * com.datahub.util.exception.RetryLimitReached}.
 */
public class AspectWriteDisabledException extends IllegalStateException {

  public AspectWriteDisabledException(@Nonnull String message) {
    super(message);
  }
}
