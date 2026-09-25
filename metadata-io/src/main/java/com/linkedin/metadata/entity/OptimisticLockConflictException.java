package com.linkedin.metadata.entity;

import jakarta.persistence.PersistenceException;
import javax.annotation.Nonnull;

/**
 * Thrown when an optimistic-locking conditional UPDATE affects 0 rows because the {@code
 * SystemMetadata.version} guard on the version-0 row no longer matches the value the writer read.
 * Caught by {@link AspectDao#runInTransactionWithRetry} (it extends {@link PersistenceException})
 * so the existing retry loop re-reads the latest state, recomputes the upsert, and re-issues the
 * conditional UPDATE.
 *
 * <p>Thrown only for {@link ConditionalWriteOutcome#CONFLICT}. A legitimate no-op ({@link
 * ConditionalWriteOutcome#SKIPPED_NOOP}) is not a conflict and must not be retried.
 *
 * <p>This does <b>not</b> implement "latest produced wins" for out-of-order payloads — only
 * write/write conflict detection on the version-0 row since this writer read it.
 *
 * <p><b>Phase-1 limitation:</b> content-equal / metadata-only updates deliberately keep the same
 * {@code SystemMetadata.version}. Two concurrent writers of that form can both CAS-succeed
 * (expected version unchanged), so one runId/lastObserved update can be lost. Content-changing
 * writes still advance the version and are conflict-protected. SELECT FOR UPDATE serialized all
 * writes; document this narrowing until a later phase bumps version on metadata-only changes.
 */
public class OptimisticLockConflictException extends PersistenceException {

  public OptimisticLockConflictException(@Nonnull String message) {
    super(message);
  }

  public OptimisticLockConflictException(@Nonnull String message, @Nonnull Throwable cause) {
    super(message, cause);
  }
}
