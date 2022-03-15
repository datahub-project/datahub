package com.linkedin.datahub.graphql.resolvers.dataset;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.EntityRelationships;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;


/**
 * Resolver used for resolving the Health state of a Dataset.
 *
 * Currently, the health status is calculated via the validation on a Dataset. If there are no validations found, the
 * health status will be undefined for the Dataset.
 *
 */
public class DatasetHealthResolver implements DataFetcher<CompletableFuture<Health>> {

  private static final String ASSERTS_RELATIONSHIP_NAME = "Asserts";
  private static final String ASSERTION_RUN_EVENT_SUCCESS_TYPE = "SUCCESS";
  private static final CachedHealth NO_HEALTH = new CachedHealth(false, null);

  private final GraphClient _graphClient;
  private final TimeseriesAspectService _timeseriesAspectService;

  private final Cache<String, CachedHealth> _statusCache;

  public DatasetHealthResolver(final GraphClient graphClient, final TimeseriesAspectService timeseriesAspectService) {
    _graphClient = graphClient;
    _timeseriesAspectService = timeseriesAspectService;
    _statusCache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build();
  }

  @Override
  public CompletableFuture<Health> get(final DataFetchingEnvironment environment) throws Exception {
    final Dataset parent = environment.getSource();
    return CompletableFuture.supplyAsync(() -> {
        try {
          final CachedHealth cachedStatus = _statusCache.get(parent.getUrn(), () -> (
              computeHealthStatusForDataset(parent.getUrn(), environment.getContext())));
          return cachedStatus.hasStatus ? cachedStatus.health : null;
        } catch (Exception e) {
          throw new RuntimeException("Failed to resolve dataset's health status.", e);
        }
    });
  }

  /**
   * Computes the "resolved health status" for a Dataset by
   *
   *  - fetching active (non-deleted) assertions
   *  - fetching latest assertion run for each
   *  - checking whether any of the assertions latest runs are failing
   *
   */
  private CachedHealth computeHealthStatusForDataset(final String datasetUrn, final QueryContext context) {
    final Health result = computeAssertionHealthForDataset(datasetUrn, context);
    if (result == null) {
      return NO_HEALTH;
    }
    return new CachedHealth(true, result);
  }

  /**
   * Returns the resolved "assertions health", which is currently a static function of whether the most recent run of
   * all dataset assertions has succeeded.
   *
   * @param datasetUrn the dataset to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the Dataset, null if one cannot be computed.
   */
  @Nullable
  private Health computeAssertionHealthForDataset(final String datasetUrn, final QueryContext context) {
    // Get active assertion urns
    final EntityRelationships relationships = _graphClient.getRelatedEntities(
        datasetUrn,
        ImmutableList.of(ASSERTS_RELATIONSHIP_NAME),
        RelationshipDirection.INCOMING,
        0,
        500,
        context.getActorUrn()
    );

    if (relationships.getTotal() > 0) {

      // If there are assertions defined, then we should return a non-null health for this asset.
      final Set<String> activeAssertionUrns = relationships.getRelationships()
          .stream()
          .map(relationship -> relationship.getEntity().toString()).collect(Collectors.toSet());

      final List<String> failingAssertionUrns = getFailingAssertionUrns(datasetUrn, activeAssertionUrns);

      // Finally compute & return the health.
      final Health health = new Health();
      if (failingAssertionUrns.size() > 0) {
        health.setStatus(HealthStatus.FAIL);
        health.setMessage(String.format("Dataset is failing %s/%s assertions.", failingAssertionUrns.size(),
            activeAssertionUrns.size()));
        health.setCauses(failingAssertionUrns);
      } else {
        health.setStatus(HealthStatus.PASS);
        health.setMessage("Dataset is passing all assertions.");
      }
      return health;

    }
    return null;
  }

  private List<String> getFailingAssertionUrns(final String asserteeUrn, final Set<String> candidateAssertionUrns) {
    // Query timeseries backend
    GenericTable result = _timeseriesAspectService.getAggregatedStats(
        Constants.ASSERTION_ENTITY_NAME,
        Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
        createAssertionAggregationSpecs(),
        createAssertionsFilter(asserteeUrn),
        createAssertionGroupingBuckets());
    if (!result.hasRows()) {
      // No completed assertion runs found. Return empty list.
      return Collections.emptyList();
    }
    // Create the buckets based on the result
    return resultToFailedAssertionUrns(result.getRows(), candidateAssertionUrns);
  }

  private Filter createAssertionsFilter(final String datasetUrn) {
    final Filter filter = new Filter();
    final ArrayList<Criterion> criteria = new ArrayList<>();

    // Add filter for asserteeUrn == datasetUrn
    Criterion datasetUrnCriterion =
        new Criterion().setField("asserteeUrn").setCondition(Condition.EQUAL).setValue(datasetUrn);
    criteria.add(datasetUrnCriterion);

    // Add filter for result == result
    Criterion startTimeCriterion =
        new Criterion().setField("status").setCondition(Condition.EQUAL).setValue(Constants.ASSERTION_RUN_EVENT_STATUS_COMPLETE);
    criteria.add(startTimeCriterion);

    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(
        new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))
    )));
    return filter;
  }

  private AggregationSpec[] createAssertionAggregationSpecs() {
    // Simply fetch the timestamp, result type for the assertion URN.
    AggregationSpec resultTypeAggregation =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("type");
    AggregationSpec timestampAggregation =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("timestampMillis");
    return new AggregationSpec[]{resultTypeAggregation, timestampAggregation};
  }

  private GroupingBucket[] createAssertionGroupingBuckets() {
    // String grouping bucket on "assertionUrn"
    GroupingBucket assertionUrnBucket = new GroupingBucket();
    assertionUrnBucket.setKey("assertionUrn").setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    return new GroupingBucket[]{assertionUrnBucket};
  }

  private List<String> resultToFailedAssertionUrns(final StringArrayArray rows, final Set<String> activeAssertionUrns) {
    final List<String> failedAssertionUrns = new ArrayList<>();
    for (StringArray row : rows) {
      // Result structure should be assertionUrn, event.result.type, timestampMillis
      if (row.size() != 3) {
        throw new RuntimeException(String.format(
            "Failed to fetch assertion run events from Timeseries index! Expected row of size 3, found %s", row.size()));
      }

      final String assertionUrn = row.get(0);
      final String resultType = row.get(1);

      // If assertion is "active" (not deleted) & is failing, then we report a degradation in health.
      if (activeAssertionUrns.contains(assertionUrn) && !ASSERTION_RUN_EVENT_SUCCESS_TYPE.equals(resultType)) {
        failedAssertionUrns.add(assertionUrn);
      }
    }
    return failedAssertionUrns;
  }

    @AllArgsConstructor
  private static class CachedHealth {
    private final boolean hasStatus;
    private final Health health;
  }
}