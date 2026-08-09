package com.linkedin.metadata.entity;

import javax.annotation.Nonnull;

/**
 * Thrown when an aspect write is attempted while the {@link AspectDao} is in read-only mode ({@code
 * canWrite=false}).
 *
 * <p>Deliberately <b>not</b> a {@link jakarta.persistence.PersistenceException}, so the transaction
 * retry loop does not mistake read-only mode for a transient conflict and burn retries. On the
 * optimistic-locking path it maps to {@link AspectWriteOutcome#FAILED}, never {@link
 * AspectWriteOutcome#CONFLICT}.
 */
public class AspectWriteDisabledException extends IllegalStateException {

  public AspectWriteDisabledException(@Nonnull String message) {
    super(message);
  }
}
