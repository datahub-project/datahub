package com.linkedin.usage;

import com.datahub.authentication.Authentication;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.client.BaseClient;
import com.linkedin.metadata.config.cache.client.UsageClientCacheConfig;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

public class RestliUsageClient extends BaseClient implements UsageClient {

  private static final UsageStatsRequestBuilders USAGE_STATS_REQUEST_BUILDERS =
      new UsageStatsRequestBuilders();

  private final OperationContext systemOperationContext;
  private final UsageClientCache usageClientCache;

  public RestliUsageClient(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull final Client restliClient,
      @Nonnull final BackoffPolicy backoffPolicy,
      int retryCount,
      UsageClientCacheConfig cacheConfig) {
    super(restliClient, backoffPolicy, retryCount);
    this.systemOperationContext = systemOperationContext;
    this.usageClientCache =
        UsageClientCache.builder()
            .config(cacheConfig)
            .loadFunction(
                (UsageClientCache.Key cacheKey) -> {
                  try {
                    return getUsageStats(
                        cacheKey.getResource(),
                        cacheKey.getRange(),
                        systemOperationContext.getAuthentication());
                  } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(e);
                  }
                })
            .build();
  }

  /**
   * Gets a specific version of downstream {@link EntityRelationships} for the given dataset. Using
   * cache and system authentication. Validate permissions before use!
   */
  @Nonnull
  public UsageQueryResult getUsageStats(@Nonnull String resource, @Nonnull UsageTimeRange range) {
    return usageClientCache.getUsageStats(systemOperationContext, resource, range);
  }

  /** Gets a specific version of downstream {@link EntityRelationships} for the given dataset. */
  @Nonnull
  private UsageQueryResult getUsageStats(
      @Nonnull String resource,
      @Nonnull UsageTimeRange range,
      @Nonnull Authentication authentication)
      throws RemoteInvocationException, URISyntaxException {

    final UsageStatsDoQueryRangeRequestBuilder requestBuilder =
        USAGE_STATS_REQUEST_BUILDERS
            .actionQueryRange()
            .resourceParam(resource)
            .durationParam(WindowDuration.DAY)
            .rangeFromEndParam(range);
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }
}
