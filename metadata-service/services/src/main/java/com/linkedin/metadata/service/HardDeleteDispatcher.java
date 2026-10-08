package com.linkedin.metadata.service;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Hands a hard delete to another process. Lets a deployment run the delete somewhere else instead
 * of in this process, without this module knowing where. {@code HardDeleteService} offers each
 * delete once; when it is not taken, the delete runs in this process.
 */
public interface HardDeleteDispatcher {

  /**
   * Hands a hard delete to another process; the caller has already authorized it.
   *
   * @return true when it was taken, false to run it here. Must not throw.
   */
  boolean dispatch(@Nonnull OperationContext opContext, @Nonnull HardDeleteRequest request);
}
