package com.linkedin.metadata.service;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Hands a hard delete to another process. Lets a deployment run the delete somewhere else instead
 * of in this process, without this module knowing where. {@code HardDeleteService} offers each
 * delete once; when it is not taken, the delete runs in this process.
 */
public interface HardDeleteDispatcher {

  /**
   * Hands a hard delete to another process; the caller has already authorized it.
   *
   * @return true when it was taken, false to run it here
   * @throws RuntimeException only when it cannot tell whether the delete was taken; the delete then
   *     fails and is not run here
   */
  boolean dispatch(@Nonnull OperationContext opContext, @Nonnull HardDeleteRequest request);

  /**
   * {@link #dispatch} for one delete of a request that deletes several entities (the bulk OpenAPI
   * delete), so a dispatcher can hand a batch over when it keeps a single delete here.
   */
  default boolean dispatchOneOfMany(
      @Nonnull OperationContext opContext, @Nonnull HardDeleteRequest request) {
    return dispatch(opContext, request);
  }

  /** How {@link #offer} left a delete. */
  enum Outcome {
    /** Not taken: the delete runs in this process. */
    DECLINED,
    /** Handed to another process, which has not run it yet. */
    QUEUED,
    /** Taken and already done by the dispatcher before it returned. */
    DONE
  }

  /**
   * What {@link #offer} did with a delete.
   *
   * @param references the references the dispatcher counted while deciding, or null when it did not
   *     count them; it can see referrers the graph no longer has (their edges went with an earlier
   *     entity delete), and it is a lower bound when the delete went over its limit
   */
  record Offer(@Nonnull Outcome outcome, @Nullable Integer references) {
    @Nonnull
    public static Offer of(@Nonnull final Outcome outcome) {
      return new Offer(outcome, null);
    }
  }

  /**
   * {@link #dispatch}, saying whether a taken delete was queued or already done. The default reads
   * every taken delete as queued and counts nothing.
   */
  @Nonnull
  default Offer offer(@Nonnull OperationContext opContext, @Nonnull HardDeleteRequest request) {
    return Offer.of(dispatch(opContext, request) ? Outcome.QUEUED : Outcome.DECLINED);
  }
}
