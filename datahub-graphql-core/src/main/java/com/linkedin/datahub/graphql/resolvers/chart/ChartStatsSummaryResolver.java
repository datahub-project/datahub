package com.linkedin.datahub.graphql.resolvers.chart;

import com.linkedin.datahub.graphql.generated.ChartStatsSummary;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChartStatsSummaryResolver
    implements DataFetcher<CompletableFuture<ChartStatsSummary>> {

  private final TimeseriesAspectService timeseriesAspectService;

  public ChartStatsSummaryResolver(final TimeseriesAspectService timeseriesAspectService) {
    this.timeseriesAspectService = timeseriesAspectService;
  }

  @Override
  public CompletableFuture<ChartStatsSummary> get(DataFetchingEnvironment environment)
      throws Exception {
    // Not yet implemented
    return CompletableFuture.completedFuture(null);
  }
}
