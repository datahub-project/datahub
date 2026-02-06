package com.linkedin.datahub.graphql.resolvers.monitor;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.MonitorError;
import com.linkedin.datahub.graphql.resolvers.assertion.AssertionErrorMessageMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

public class MonitorErrorRecommendedActionResolver
    implements DataFetcher<CompletableFuture<String>> {

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) {
    final MonitorError error = (MonitorError) environment.getSource();
    return GraphQLConcurrencyUtils.supplyAsync(
        () ->
            error != null
                ? AssertionErrorMessageMapper.recommendedActionForMonitorError(error.getType())
                : null,
        this.getClass().getSimpleName(),
        "get");
  }
}
