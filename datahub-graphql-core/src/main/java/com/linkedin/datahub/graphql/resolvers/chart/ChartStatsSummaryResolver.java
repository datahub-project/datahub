package com.linkedin.datahub.graphql.resolvers.chart;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ChartStatsSummary;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChartStatsSummaryResolver
    implements DataFetcher<CompletableFuture<ChartStatsSummary>> {

  private final TimeseriesAspectService timeseriesAspectService;
  private final Cache<Urn, ChartStatsSummary> summaryCache;

  public ChartStatsSummaryResolver(final TimeseriesAspectService timeseriesAspectService) {
    this.timeseriesAspectService = timeseriesAspectService;
    this.summaryCache =
        CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(6, TimeUnit.HOURS).build();
  }

  @Override
  public CompletableFuture<ChartStatsSummary> get(DataFetchingEnvironment environment)
      throws Exception {
    // Not yet implemented
    return CompletableFuture.completedFuture(null);
  }
}
