package com.linkedin.metadata.client;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.common.WindowDuration;
import com.linkedin.metadata.config.cache.client.UsageClientCacheConfig;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.UsageServiceUtil;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.usage.UsageClient;
import com.linkedin.usage.UsageClientCache;
import com.linkedin.usage.UsageQueryResult;
import com.linkedin.usage.UsageTimeRange;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

public class UsageStatsJavaClient implements UsageClient {

  private final UsageClientCache usageClientCache;
  private final TimeseriesAspectService timeseriesAspectService;
  private final Cache<String, OperationContext> operationContextMap;

  public UsageStatsJavaClient(
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull UsageClientCacheConfig cacheConfig) {
    this.timeseriesAspectService = timeseriesAspectService;
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
                        cacheKey.getRange());
                  } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(e);
                  }
                })
            .build();
  }

  @Nonnull
  @Override
  public UsageQueryResult getUsageStats(
      @Nonnull OperationContext opContext,
      @Nonnull String resource,
      @Nonnull UsageTimeRange range) {
    operationContextMap.put(opContext.getEntityContextId(), opContext);
    return usageClientCache.getUsageStats(opContext, resource, range);
  }

  @Nonnull
  @Override
  public UsageQueryResult getUsageStatsNoCache(
      @Nonnull OperationContext opContext, @Nonnull String resource, @Nonnull UsageTimeRange range)
      throws RemoteInvocationException, URISyntaxException {

    return UsageServiceUtil.queryRange(
        opContext, timeseriesAspectService, resource, WindowDuration.DAY, range);
  }
}
