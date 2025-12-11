/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.chart;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
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
    return GraphQLConcurrencyUtils.supplyAsync(() -> null, this.getClass().getSimpleName(), "get");
  }
}
