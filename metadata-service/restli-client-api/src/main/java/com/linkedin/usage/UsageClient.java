package com.linkedin.usage;

import com.linkedin.common.EntityRelationships;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface UsageClient {
  /**
   * Gets a specific version of downstream {@link EntityRelationships} for the given dataset. Using
   * cache and system authentication. Validate permissions before use!
   */
  @Nonnull
  UsageQueryResult getUsageStats(
      @Nonnull OperationContext opContext,
      @Nonnull String resource,
      @Nonnull UsageTimeRange range,
      @Nullable Long startTimeMillis,
      @Nullable String timeZone);

  @Nonnull
  UsageQueryResult getUsageStatsNoCache(
      @Nonnull OperationContext opContext,
      @Nonnull String resource,
      @Nonnull UsageTimeRange range,
      @Nullable Long startTimeMillis,
      @Nullable String timeZone)
      throws RemoteInvocationException, URISyntaxException;
}
