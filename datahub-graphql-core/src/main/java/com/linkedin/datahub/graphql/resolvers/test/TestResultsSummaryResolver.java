package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.TestResultsSummary;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.*;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.util.TestUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.test.BatchTestRunEvent;
import com.linkedin.test.TestInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** GraphQL Resolver used for fetching the results summary for a given test. */
@Slf4j
public class TestResultsSummaryResolver
    implements DataFetcher<CompletableFuture<TestResultsSummary>> {

  // The max number of aggregations when fetching test results.
  // If a customer ever has more than 100 tests this WILL NEED TO BE INCREASED.
  static final int MAX_AGGREGATION_LIMIT = 100;

  private static final String TEST_DEFINITION_MD5_FIELD = "testDefinitionsMd5";

  private final EntitySearchService entitySearchService;
  private final EntityService entityService;

  private final TimeseriesAspectService timeseriesAspectService;

  public TestResultsSummaryResolver(
      @Nonnull final EntitySearchService entitySearchService,
      @Nonnull final EntityService entityService,
      @Nonnull final TimeseriesAspectService timeseriesAspectService) {
    this.entitySearchService =
        Objects.requireNonNull(entitySearchService, "entitySearchService must not be null");
    this.entityService = Objects.requireNonNull(entityService, "entityService must not be null");
    this.timeseriesAspectService =
        Objects.requireNonNull(timeseriesAspectService, "timeseriesAspectService must not be null");
  }

  @Override
  public CompletableFuture<TestResultsSummary> get(DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final Urn testUrn = Urn.createFromString(((Test) environment.getSource()).getUrn());
    final TestInfo testInfo =
        (TestInfo)
            entityService.getLatestAspect(
                context.getOperationContext(), testUrn, TEST_INFO_ASPECT_NAME);

    String md5 = testInfo == null ? null : testInfo.getDefinition().getMd5();

    return CompletableFuture.supplyAsync(
        () -> {
          List<EnvelopedAspect> lastComputed =
              timeseriesAspectService.getAspectValues(
                  context.getOperationContext(),
                  testUrn,
                  TEST_ENTITY_NAME,
                  BATCH_TEST_RUN_EVENT_ASPECT_NAME,
                  null,
                  null,
                  1,
                  createFilter());
          Long timestamp = null;
          if (!lastComputed.isEmpty()) {
            EnvelopedAspect envelopedAspect = lastComputed.get(0);
            BatchTestRunEvent batchTestRunEvent =
                GenericRecordUtils.deserializeAspect(
                    envelopedAspect.getAspect().getValue(),
                    "application/json",
                    BatchTestRunEvent.class);
            timestamp = batchTestRunEvent.getTimestampMillis();
          }
          long passingCount =
              getResultsCount(
                  context.getOperationContext(),
                  testUrn,
                  PASSING_TESTS_FIELD,
                  () ->
                      TestUtils.buildTestPassingFilter(
                          testUrn, md5, context.getOperationContext().getAspectRetriever()));
          long failingCount =
              getResultsCount(
                  context.getOperationContext(),
                  testUrn,
                  FAILING_TESTS_FIELD,
                  () ->
                      TestUtils.buildTestFailingFilter(
                          testUrn, md5, context.getOperationContext().getAspectRetriever()));
          final TestResultsSummary result = new TestResultsSummary();
          result.setPassingCount(passingCount);
          result.setFailingCount(failingCount);
          result.setTestDefinitionMd5(md5);
          if (timestamp != null) {
            result.setLastRunTimestampMillis(timestamp);
          }
          return result;
        });
  }

  public static Filter createFilter() {
    Filter filter = new Filter();
    final ArrayList<Criterion> criteria = new ArrayList<>();
    Criterion partitionCriterion =
        CriterionUtils.buildCriterion("partition", Condition.EQUAL, "FULL");
    criteria.add(partitionCriterion);
    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));
    return filter;
  }

  private long getResultsCount(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn testUrn,
      @Nonnull final String fieldName,
      @Nonnull Supplier<Filter> filterSupplier) {
    try {
      final Map<String, Long> aggregations =
          this.entitySearchService.aggregateByValue(
              opContext, null, fieldName, filterSupplier.get(), MAX_AGGREGATION_LIMIT);
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
}
