package com.linkedin.datahub.graphql.resolvers.dataset;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.EntityRelationships;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;


/**
 * Resolver used for resolving the Health state of a Dataset.
 *
 * Currently, the health status is calculated via the validation on a Dataset. If there are no validations found, the
 * health status will be undefined for the Dataset.
 *
 * TODO: We should consider caching the
 */
public class DatasetHealthResolver implements DataFetcher<CompletableFuture<Health>> {

  private static final String SUCCEEDED_RESULT_TYPE = "SUCCESS";
  private static final CachedHealth NO_HEALTH = new CachedHealth(false, null);

  private final GraphClient _graphClient;
  private final TimeseriesAspectService _timeseriesAspectService;

  private final Cache<String, CachedHealth> _statusCache;

  public DatasetHealthResolver(final GraphClient graphClient, final TimeseriesAspectService timeseriesAspectService) {
    _graphClient = graphClient;
    _timeseriesAspectService = timeseriesAspectService;
    _statusCache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(10, TimeUnit.MINUTES)
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
   *  -
   *
   *  TODO: Migrate to using the Time Series API for fetching from Assertion Run Events
   *
   */
  private CachedHealth computeHealthStatusForDataset(final String datasetUrn, final QueryContext context) {

    // Get active assertion urns
    final EntityRelationships relationships = _graphClient.getRelatedEntities(
      datasetUrn,
      ImmutableList.of("Asserts"),
      RelationshipDirection.INCOMING,
      0,
      500,
        context.getActor().toUrnStr()
    );

    if (relationships.getTotal() > 0) {

      final Set<String> activeAssertionUrns = relationships.getRelationships()
          .stream()
          .map(relationship ->
            relationship.getEntity().toString()
          ).collect(Collectors.toSet());

      // Only continue if the dataset has assertions.
      Filter filter = new Filter();
      ArrayList<Criterion> criteria = new ArrayList<>();

      // Add filter for asserteeUrn == datasetUrn
      Criterion datasetUrnCriterion = new Criterion().setField("asserteeUrn").setCondition(Condition.EQUAL).setValue(datasetUrn);
      criteria.add(datasetUrnCriterion);

      // Add filter for result == result
      Criterion startTimeCriterion = new Criterion().setField("status").setCondition(Condition.EQUAL).setValue("COMPLETE");
      criteria.add(startTimeCriterion);

      // Simply fetch the timestamp, result type for the assertion URN.
      AggregationSpec resultTypeAggregation = new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("type");
      AggregationSpec timestampAggregation = new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("timestampMillis");
      AggregationSpec[] aggregationSpecs = new AggregationSpec[]{resultTypeAggregation, timestampAggregation};

      // String grouping bucket on "assertionUrn"
      GroupingBucket assertionUrnBucket = new GroupingBucket();
      assertionUrnBucket.setKey("assertionUrn").setType(GroupingBucketType.STRING_GROUPING_BUCKET);
      GroupingBucket[] groupingBuckets = new GroupingBucket[]{assertionUrnBucket};

      // Query backend
      GenericTable result = _timeseriesAspectService.getAggregatedStats(
              "assertion",
              "assertionRunEvent",
              aggregationSpecs,
              filter,
          groupingBuckets);

      if (!result.hasRows()) {
        // No completed assertion runs found.
        return NO_HEALTH;
      }

      // Create the buckets based on the result
      final List<String> failedAssertionUrns = new ArrayList<>();
      for (StringArray row : result.getRows()) {
        // Result structure should be assertionUrn, event.result.type, timestampMillis
        if (row.size() != 3) {
          throw new RuntimeException(String.format(
              "Failed to fetch assertion run events from Timeseries index! Expected row of size 3, found %s", row.size()));
        }

        final String assertionUrn = row.get(0);
        final String resultType = row.get(1);

        // If assertion is "active" (not deleted) & is failing, then let's report a degradation in health.
        if (activeAssertionUrns.contains(assertionUrn) && !SUCCEEDED_RESULT_TYPE.equals(resultType)) {
          failedAssertionUrns.add(assertionUrn);
        }
      }

      // Finally compute & return the health.
      final Health health = new Health();
      if (failedAssertionUrns.size() > 0) {
        health.setStatus(HealthStatus.FAIL);
        health.setMessage(String.format("Dataset is failing %s/%s assertions.", failedAssertionUrns.size(), activeAssertionUrns.size()));
        health.setCauses(failedAssertionUrns);
      } else {
        health.setStatus(HealthStatus.PASS);
        health.setMessage(String.format("Dataset is passing %s/%s assertions.", activeAssertionUrns.size(), activeAssertionUrns.size()));
      }
      return new CachedHealth(true, health);
    }
    return NO_HEALTH;
  }

  @AllArgsConstructor
  private static class CachedHealth {
    private final boolean hasStatus;
    private final Health health;
  }
}