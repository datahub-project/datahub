package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.test.TestResults;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

/** Runs tests for a given entity. */
@RequiredArgsConstructor
public class RunTestsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private static final String MODE_ARG_NAME = "mode";
  private final TestEngine _testEngine;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final String urnStr = environment.getArgument("urn");
          final Urn urn = UrnUtils.getUrn(urnStr);
          final TestEngine.EvaluationMode evaluationMode =
              environment.getArgument(MODE_ARG_NAME) != null
                  ? TestEngine.EvaluationMode.valueOf(environment.getArgument(MODE_ARG_NAME))
                  : TestEngine.EvaluationMode.DEFAULT;

          TestResults testResults =
              _testEngine.evaluateTests(context.getOperationContext(), urn, evaluationMode);

          return testResults.getFailing().isEmpty();
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
