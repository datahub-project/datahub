package com.linkedin.metadata.service;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Hands an ingestion rollback to another process. Lets a deployment run the rollback somewhere else
 * instead of in this process, without this module knowing where. {@link RollbackService} offers
 * each rollback once, after authorizing it; when it is not taken, the rollback runs in this
 * process.
 */
public interface IngestionRollbackDispatcher {

  /**
   * Hands a rollback of {@code runId} to another process; the caller has already authorized it.
   *
   * @return true when it was taken, false to run it here; never throws
   */
  boolean dispatch(@Nonnull OperationContext opContext, @Nonnull String runId, boolean hardDelete);
}
