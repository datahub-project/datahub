package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.metadata.AcrylConstants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.TestResultsSummary;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.utils.ESUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** GraphQL Resolver used for fetching the results summary for a given test. */
@Slf4j
public class TestResultsSummaryResolver
    implements DataFetcher<CompletableFuture<TestResultsSummary>> {

  // The max number of aggregations when fetching test results.
  // If a customer ever has more than 100 tests this WILL NEED TO BE INCREASED.
  static final int MAX_AGGREGATION_LIMIT = 100;

  private final EntitySearchService entitySearchService;

  public TestResultsSummaryResolver(@Nonnull final EntitySearchService entitySearchService) {
    this.entitySearchService =
        Objects.requireNonNull(entitySearchService, "entitySearchService must not be null");
  }

  @Override
  public CompletableFuture<TestResultsSummary> get(DataFetchingEnvironment environment)
      throws Exception {
    final Urn testUrn = Urn.createFromString(((Test) environment.getSource()).getUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          long passingCount = getResultsCount(testUrn, PASSING_TESTS_FIELD);
          long failingCount = getResultsCount(testUrn, FAILING_TESTS_FIELD);
          final TestResultsSummary result = new TestResultsSummary();
          result.setPassingCount(passingCount);
          result.setFailingCount(failingCount);
          return result;
        });
  }

  private long getResultsCount(@Nonnull final Urn testUrn, @Nonnull final String fieldName) {
    try {
      final Map<String, Long> aggregations =
          this.entitySearchService.aggregateByValue(
              null, fieldName, buildFilter(testUrn, fieldName), MAX_AGGREGATION_LIMIT);
      return aggregations.getOrDefault(testUrn.toString(), 0L);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to fetch the test result counts for test with urn %s! Returning 0 results.",
              testUrn),
          e);
      return 0L;
    }
  }

  private Filter buildFilter(@Nonnull final Urn testUrn, @Nonnull final String fieldName) {
    final Filter result = new Filter();
    final String fieldNameWithSuffix = ESUtils.toKeywordField(fieldName, false);
    result.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            ImmutableList.of(
                                new Criterion()
                                    .setNegated(false)
                                    .setField(fieldNameWithSuffix)
                                    .setValues(
                                        new StringArray(ImmutableList.of(testUrn.toString())))
                                    .setValue(testUrn.toString()) // :-(
                                    .setCondition(Condition.EQUAL)))))));
    return result;
  }
}
