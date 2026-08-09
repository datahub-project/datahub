package com.linkedin.metadata.entity;

import jakarta.persistence.PersistenceException;
import javax.annotation.Nonnull;

/**
 * Signals that an optimistic-locking conditional UPDATE affected zero rows because the {@code
 * SystemMetadata.version} guard on the version-0 row no longer matched the value this writer read,
 * or that two writers raced to insert the same version-0 row.
 *
 * <p>Extends {@link PersistenceException} so the legacy transaction retry loop treats it as a
 * transient failure. In the batch write path (Stage 1+) conflicts are represented as data via {@link
 * AspectWriteOutcome#CONFLICT}; this exception is retained only for the version-0 insert race, where
 * a genuine duplicate-key must abort the transaction (notably on Postgres) and be re-driven.
 */
public class OptimisticLockConflictException extends PersistenceException {

  public OptimisticLockConflictException(@Nonnull String message) {
    super(message);
  }

  public OptimisticLockConflictException(@Nonnull String message, @Nonnull Throwable cause) {
    super(message, cause);
  }
}
