package com.linkedin.datahub.graphql.resolvers.test;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.TestResult;
import com.linkedin.datahub.graphql.generated.TestResultType;
import com.linkedin.datahub.graphql.generated.TestResults;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** GraphQL Resolver used for fetching the list of tests for an entity */
@Slf4j
public class TestResultsResolver implements DataFetcher<CompletableFuture<TestResults>> {

  private final EntityClient _entityClient;

  public TestResultsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<TestResults> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn entityUrn = Urn.createFromString(((Entity) environment.getSource()).getUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          final com.linkedin.test.TestResults gmsTestResults = getTestResults(entityUrn, context);

          if (gmsTestResults == null) {
            return null;
          }

          TestResults testResults = new TestResults();
          testResults.setPassing(mapTestResults(gmsTestResults.getPassing()));
          testResults.setFailing(mapTestResults(gmsTestResults.getFailing()));
          return testResults;
        });
  }

  @Nullable
  private com.linkedin.test.TestResults getTestResults(
      final Urn entityUrn, final QueryContext context) {
    try {
      final EntityResponse entityResponse =
          _entityClient.getV2(
              entityUrn.getEntityType(),
              entityUrn,
              ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME),
              context.getAuthentication());
      if (entityResponse.hasAspects()
          && entityResponse.getAspects().containsKey(Constants.TEST_RESULTS_ASPECT_NAME)) {
        return new com.linkedin.test.TestResults(
            entityResponse.getAspects().get(Constants.TEST_RESULTS_ASPECT_NAME).getValue().data());
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get test results", e);
    }
  }

  private List<TestResult> mapTestResults(
      final @Nonnull List<com.linkedin.test.TestResult> gmsResults) {
    final List<TestResult> results = new ArrayList<>();
    for (com.linkedin.test.TestResult gmsResult : gmsResults) {
      results.add(mapTestResult(gmsResult));
    }
    return results;
  }

  private TestResult mapTestResult(final @Nonnull com.linkedin.test.TestResult gmsResult) {
    final TestResult testResult = new TestResult();
    final Test partialTest = new Test();
    partialTest.setUrn(gmsResult.getTest().toString());
    testResult.setTest(partialTest);
    testResult.setType(TestResultType.valueOf(gmsResult.getType().toString()));
    return testResult;
  }
}
