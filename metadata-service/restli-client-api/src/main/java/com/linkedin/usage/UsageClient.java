package com.linkedin.usage;

import com.linkedin.common.EntityRelationships;
import javax.annotation.Nonnull;

public interface UsageClient {
  /**
   * Gets a specific version of downstream {@link EntityRelationships} for the given dataset. Using
   * cache and system authentication. Validate permissions before use!
   */
  @Nonnull
  UsageQueryResult getUsageStats(@Nonnull String resource, @Nonnull UsageTimeRange range);
}
