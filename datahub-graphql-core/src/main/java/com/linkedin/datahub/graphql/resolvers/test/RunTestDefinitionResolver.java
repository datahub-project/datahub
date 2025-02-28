package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.RunTestDefinitionResult;
import com.linkedin.datahub.graphql.generated.RunTestDefinitionStatus;
import com.linkedin.datahub.graphql.generated.TestDefinitionInput;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.definition.TestDefinition;
import com.linkedin.test.TestResults;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

/** Runs tests for a given entity. */
@RequiredArgsConstructor
public class RunTestDefinitionResolver
    implements DataFetcher<CompletableFuture<RunTestDefinitionResult>> {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:test:test");
  private final TestEngine _testEngine;

  @Override
  public CompletableFuture<RunTestDefinitionResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final Urn urn = UrnUtils.getUrn(urnStr);
    final TestDefinitionInput test =
        bindArgument(environment.getArgument("test"), TestDefinitionInput.class);
    return CompletableFuture.supplyAsync(
        () -> {
          final TestDefinition parsedDefinition =
              _testEngine.getParser().deserialize(TEST_URN, test.getJson());
          final TestResults testResults =
              _testEngine
                  .evaluateTests(
                      context.getOperationContext(),
                      Set.of(urn),
                      ImmutableSet.of(parsedDefinition),
                      TestEngine.EvaluationMode.EVALUATE_ONLY)
                  .get(urn);
          return mapToResult(testResults);
        });
  }

  private RunTestDefinitionResult mapToResult(final TestResults testResults) {
    final RunTestDefinitionResult result = new RunTestDefinitionResult();
    final RunTestDefinitionStatus status;
    if (testResults.getFailing().stream().anyMatch(res -> TEST_URN.equals(res.getTest()))) {
      status = RunTestDefinitionStatus.FAIL;
    } else if (testResults.getPassing().stream().anyMatch(res -> TEST_URN.equals(res.getTest()))) {
      status = RunTestDefinitionStatus.PASS;
    } else {
      // No result. The test was not evaluated.
      status = RunTestDefinitionStatus.NONE;
    }
    result.setStatus(status);
    return result;
  }
}
