package com.linkedin.usage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.client.BaseClient;
import com.linkedin.common.client.restli.RestliRequestContextResolver;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.metadata.config.cache.client.UsageClientCacheConfig;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RestliUsageClient extends BaseClient implements UsageClient {
  private static final UsageStatsRequestBuilders USAGE_STATS_REQUEST_BUILDERS =
      new UsageStatsRequestBuilders();

  private final UsageClientCache usageClientCache;

  @Nonnull private final Cache<String, OperationContext> operationContextMap;

  /**
   * Legacy no-resolver constructor — used by tests and OSS paths that don't need outbound request
   * decoration. Defaults to a pass-through resolver inside {@link BaseClient}. Production wiring
   * should prefer the overload that accepts a {@link RestliRequestContextResolver}.
   */
  public RestliUsageClient(
      @Nonnull final Client restliClient,
      @Nonnull final BackoffPolicy backoffPolicy,
      int retryCount,
      UsageClientCacheConfig cacheConfig,
      MetricUtils metricUtils) {
    this(restliClient, backoffPolicy, retryCount, cacheConfig, metricUtils, null);
  }

  public RestliUsageClient(
      @Nonnull final Client restliClient,
      @Nonnull final BackoffPolicy backoffPolicy,
      int retryCount,
      UsageClientCacheConfig cacheConfig,
      MetricUtils metricUtils,
      @Nullable final RestliRequestContextResolver restliRequestContextResolver) {
    super(
        restliClient,
        EntityClientConfig.builder().backoffPolicy(backoffPolicy).retryCount(retryCount).build(),
        restliRequestContextResolver != null
            ? restliRequestContextResolver
            : new RestliRequestContextResolver(java.util.Collections.emptyList()));
    this.operationContextMap = Caffeine.newBuilder().maximumSize(500).build();
    this.usageClientCache =
        UsageClientCache.builder()
            .config(cacheConfig)
            .loadFunction(
                (UsageClientCache.Key cacheKey) -> {
                  try {
                    return getUsageStatsNoCache(
                        operationContextMap.getIfPresent(cacheKey.getContextId()),
                        cacheKey.getResource(),
                        cacheKey.getRange(),
                        cacheKey.getStartTimeMillis(),
                        cacheKey.getTimeZone());
                  } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(e);
                  }
                })
            .build(metricUtils);
  }

  /**
   * Gets a specific version of downstream {@link EntityRelationships} for the given dataset. Using
   * cache and system authentication. Validate permissions before use!
   */
  @Nonnull
  public UsageQueryResult getUsageStats(
      @Nonnull OperationContext opContext,
      @Nonnull String resource,
      @Nonnull UsageTimeRange range,
      @Nullable Long startTimeMillis,
      @Nullable String timeZone) {
    operationContextMap.put(opContext.getEntityContextId(), opContext);
    return usageClientCache.getUsageStats(opContext, resource, range, startTimeMillis, timeZone);
  }

  /** Gets a specific version of downstream {@link EntityRelationships} for the given dataset. */
  @Override
  @Nonnull
  public UsageQueryResult getUsageStatsNoCache(
      @Nonnull OperationContext opContext,
      @Nonnull String resource,
      @Nonnull UsageTimeRange range,
      @Nullable Long startTimeMillis,
      @Nullable String timeZone)
      throws RemoteInvocationException, URISyntaxException {

    final UsageStatsDoQueryRangeRequestBuilder requestBuilder =
        USAGE_STATS_REQUEST_BUILDERS
            .actionQueryRange()
            .resourceParam(resource)
            .durationParam(WindowDuration.DAY)
            .rangeFromEndParam(range);
    return sendClientRequest(requestBuilder, opContext).getEntity();
  }
}
