package com.linkedin.metadata.aspect.consistency.fix;

import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Interface for applying fixes to resolve consistency issues.
 *
 * <p>Each implementation handles a specific {@link ConsistencyFixType}. The fix uses data from the
 * {@link ConsistencyIssue} to perform the necessary operations.
 *
 * <p>Implementations should be stateless and thread-safe.
 */
public interface ConsistencyFix {

  /**
   * Get the fix type this implementation handles.
   *
   * @return fix type
   */
  @Nonnull
  ConsistencyFixType getType();

  /**
   * Apply the fix for an issue.
   *
   * @param opContext operation context
   * @param issue the issue to fix (contains all necessary data)
   * @param dryRun if true, only report what would be done without making changes
   * @return detail of the fix operation
   */
  @Nonnull
  ConsistencyFixDetail apply(
      @Nonnull OperationContext opContext, @Nonnull ConsistencyIssue issue, boolean dryRun);
}
