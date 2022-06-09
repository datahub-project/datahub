package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.test.TestResults;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;


/**
 * Runs tests for a given entity.
 */
@RequiredArgsConstructor
public class RunTestsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final TestEngine _testEngine;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {

    return CompletableFuture.supplyAsync(() -> {

      final String urnStr = environment.getArgument("urn");
      final Urn urn = UrnUtils.getUrn(urnStr);
      TestResults testResults = _testEngine.evaluateTestsForEntity(urn, true);

      return testResults.getFailing().isEmpty();
    });
  }
}
