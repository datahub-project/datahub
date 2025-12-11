/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
