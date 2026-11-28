package com.linkedin.metadata.aspect.consistency.fix;

import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Extension of {@link ConsistencyFix} for fixes that execute across a batch of issues together (for
 * example batching primary-storage reads and Kafka flushes) rather than one issue at a time.
 */
public interface BatchConsistencyFix extends ConsistencyFix {

  /**
   * Apply the fix across a batch of issues of {@link #getType()}, returning one {@link
   * ConsistencyFixDetail} per input issue in the exact input order.
   */
  @Nonnull
  List<ConsistencyFixDetail> applyBatch(
      @Nonnull OperationContext opContext, @Nonnull List<ConsistencyIssue> issues, boolean dryRun);
}
