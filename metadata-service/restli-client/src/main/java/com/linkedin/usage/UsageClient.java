package com.linkedin.usage;

import com.datahub.authentication.Authentication;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.client.BaseClient;
import com.linkedin.metadata.config.cache.client.UsageClientCacheConfig;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

public class UsageClient extends BaseClient {

  private static final UsageStatsRequestBuilders USAGE_STATS_REQUEST_BUILDERS =
      new UsageStatsRequestBuilders();

  private final UsageClientCache usageClientCache;

  public UsageClient(
      @Nonnull final Client restliClient,
      @Nonnull final BackoffPolicy backoffPolicy,
      int retryCount,
      Authentication systemAuthentication,
      UsageClientCacheConfig cacheConfig) {
    super(restliClient, backoffPolicy, retryCount);
    this.usageClientCache =
        UsageClientCache.builder()
            .config(cacheConfig)
            .loadFunction(
                (String resource, UsageTimeRange range) -> {
                  try {
                    return getUsageStats(resource, range, systemAuthentication);
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
    return usageClientCache.getUsageStats(resource, range);
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
