package com.linkedin.metadata.entity.coordinator;

import javax.annotation.Nonnull;

/**
 * Executes the authoritative DB transaction for a coordinated command: take a single
 * globally-sorted {@code FOR UPDATE} lock over {@code plan}'s keyset, verify the observed versions,
 * and apply the planned mutations.
 *
 * <p>This is the correctness seam; the coordinator's distributed {@code IMap} lock is only a
 * fast-path serializer. A thrown exception (e.g. {@code RetryLimitReached}, a stale-version
 * conflict) propagates to the caller unchanged.
 *
 * @param <T> the result type of the commit
 */
@FunctionalInterface
public interface CoordinatedCommit<T> {

  /** Runs the DB transaction under a single sorted lock and returns its result. */
  T commitUnderLock(@Nonnull MutationPlan plan) throws Exception;
}
