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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * GraphQL Resolver used for fetching the list of tests for an entity
 */
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

    return CompletableFuture.supplyAsync(() -> {

      final com.linkedin.test.TestResults gmsTestResults = getTestResults(entityUrn, context);

      if (gmsTestResults == null) {
        return null;
      }

      TestResults testResults = new TestResults();
      testResults.setPassing(mapTestResults(gmsTestResults.getPassing(), context));
      testResults.setFailing(mapTestResults(gmsTestResults.getFailing(), context));
      return testResults;
    });
  }

  @Nullable
  private com.linkedin.test.TestResults getTestResults(final Urn entityUrn, final QueryContext context) {
    try {
      final EntityResponse entityResponse =
          _entityClient.getV2(entityUrn.getEntityType(), entityUrn, ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME),
              context.getAuthentication());
      if (entityResponse.hasAspects() && entityResponse.getAspects().containsKey(Constants.TEST_RESULTS_ASPECT_NAME)) {
        return new com.linkedin.test.TestResults(
            entityResponse.getAspects().get(Constants.TEST_RESULTS_ASPECT_NAME).getValue().data());
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get test results", e);
    }
  }

  private List<TestResult> mapTestResults(final @Nonnull List<com.linkedin.test.TestResult> gmsResults,
      final QueryContext context) {
    final List<TestResult> results = new ArrayList<>();
    final Set<Urn> testUrns =
        gmsResults.stream().map(com.linkedin.test.TestResult::getTest).collect(Collectors.toSet());
    Set<Urn> existingTestUrns = getExistingTestUrns(testUrns, context);
    for (com.linkedin.test.TestResult gmsResult : gmsResults) {
      // Make sure we add only the existing tests (filter out deleted)
      if (existingTestUrns.contains(gmsResult.getTest())) {
        results.add(mapTestResult(gmsResult));
      }
    }
    return results;
  }

  // Return the set of test urns amongst the input test urns that actually exist a.k.a not deleted
  private Set<Urn> getExistingTestUrns(final Set<Urn> testUrns, final QueryContext context) {
    Map<Urn, EntityResponse> batchGetResponse;
    try {
      batchGetResponse = _entityClient.batchGetV2(Constants.TEST_ENTITY_NAME, testUrns,
          Collections.singleton(Constants.TEST_INFO_ASPECT_NAME), context.getAuthentication());
    } catch (Exception e) {
      log.error("Error while fetching test info aspects for the given test results", e);
      return testUrns;
    }
    return batchGetResponse.entrySet()
        .stream()
        .filter(entry -> isNotEmpty(entry.getValue()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  private boolean isNotEmpty(EntityResponse entityResponse) {
    return entityResponse.getAspects().containsKey(Constants.TEST_INFO_ASPECT_NAME);
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