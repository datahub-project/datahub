package com.linkedin.datahub.graphql.resolvers.assertion;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AssertionResultError;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

public class AssertionResultErrorRecommendedActionResolver
    implements DataFetcher<CompletableFuture<String>> {

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) {
    final AssertionResultError error = (AssertionResultError) environment.getSource();
    return GraphQLConcurrencyUtils.supplyAsync(
        () ->
            error != null
                ? AssertionErrorMessageMapper.recommendedActionForEvaluationError(error.getType())
                : null,
        this.getClass().getSimpleName(),
        "get");
  }
}
