package com.linkedin.datahub.graphql.resolvers.dashboard;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregation;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregationMetrics;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.generated.DashboardUsageQueryResult;
import com.linkedin.datahub.graphql.generated.DashboardUsageQueryResultAggregations;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.WindowDuration;
import com.linkedin.datahub.graphql.types.dashboard.mappers.DashboardUsageMetricMapper;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


/**
 * Resolver used for resolving the usage statistics of a Dashboard.
 * <p>
 * Returns daily as well as absolute usage metrics of Dashboard
 */
@Slf4j
public class DashboardUsageStatsResolver implements DataFetcher<CompletableFuture<DashboardUsageQueryResult>> {
  private static final String ES_FIELD_URN = "urn";
  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  private static final String ES_FIELD_EVENT_GRANULARITY = "eventGranularity";
  private static final String ES_NULL_VALUE = "NULL";
  private final TimeseriesAspectService timeseriesAspectService;

  public DashboardUsageStatsResolver(TimeseriesAspectService timeseriesAspectService) {
    this.timeseriesAspectService = timeseriesAspectService;
  }

  @Override
  public CompletableFuture<DashboardUsageQueryResult> get(DataFetchingEnvironment environment) throws Exception {
    final String dashboardUrn = ((Entity) environment.getSource()).getUrn();
    final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
    final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
    // Max number of aspects to return for absolute dashboard usage.
    final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);

    return CompletableFuture.supplyAsync(() -> {
      DashboardUsageQueryResult usageQueryResult = new DashboardUsageQueryResult();

      // Time Bucket Stats
      Filter bucketStatsFilter = createBucketUsageStatsFilter(dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis);
      List<DashboardUsageAggregation> dailyUsageBuckets = getBuckets(bucketStatsFilter, dashboardUrn);
      DashboardUsageQueryResultAggregations aggregations = getAggregations(bucketStatsFilter, dailyUsageBuckets);

      usageQueryResult.setBuckets(dailyUsageBuckets);
      usageQueryResult.setAggregations(aggregations);

      // Absolute usage metrics
      List<DashboardUsageMetrics> dashboardUsageMetrics =
          getDashboardUsageMetrics(dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis, maybeLimit);
      usageQueryResult.setMetrics(dashboardUsageMetrics);
      return usageQueryResult;
    });
  }

  private List<DashboardUsageMetrics> getDashboardUsageMetrics(String dashboardUrn, Long maybeStartTimeMillis,
      Long maybeEndTimeMillis, Integer maybeLimit) {
    List<DashboardUsageMetrics> dashboardUsageMetrics;
    try {
      Filter filter = new Filter();
      final ArrayList<Criterion> criteria = new ArrayList<>();

      // Add filter for absence of eventGranularity - only consider absolute stats
      Criterion excludeTimeBucketsCriterion =
          new Criterion().setField(ES_FIELD_EVENT_GRANULARITY).setCondition(Condition.IS_NULL).setValue("");
      criteria.add(excludeTimeBucketsCriterion);
      filter.setOr(new ConjunctiveCriterionArray(
          ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));

      List<EnvelopedAspect> aspects =
          timeseriesAspectService.getAspectValues(Urn.createFromString(dashboardUrn), Constants.DASHBOARD_ENTITY_NAME,
              Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME, maybeStartTimeMillis, maybeEndTimeMillis, maybeLimit,
              null, filter);
      dashboardUsageMetrics = aspects.stream().map(DashboardUsageMetricMapper::map).collect(Collectors.toList());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid resource", e);
    }
    return dashboardUsageMetrics;
  }

  private DashboardUsageQueryResultAggregations getAggregations(Filter bucketStatsFilter,
      final List<DashboardUsageAggregation> dailyUsageBuckets) {
    List<DashboardUserUsageCounts> userUsageCounts = getUserUsageCounts(bucketStatsFilter);
    DashboardUsageQueryResultAggregations aggregations = new DashboardUsageQueryResultAggregations();
    aggregations.setUsers(userUsageCounts);
    aggregations.setUniqueUserCount(userUsageCounts.size());

    // Compute total viewsCount and executionsCount for queries time range from the buckets itself.
    // We want to avoid issuing an additional query with a sum aggregation.
    Integer totalViewsCount = null;
    Integer totalExecutionsCount = null;
    for (DashboardUsageAggregation bucket : dailyUsageBuckets) {
      if (bucket.getMetrics().getExecutionsCount() != null) {
        if (totalExecutionsCount == null) {
          totalExecutionsCount = 0;
        }
        totalExecutionsCount += bucket.getMetrics().getExecutionsCount();
      }
      if (bucket.getMetrics().getViewsCount() != null) {
        if (totalViewsCount == null) {
          totalViewsCount = 0;
        }
        totalViewsCount += bucket.getMetrics().getViewsCount();
      }
    }

    aggregations.setExecutionsCount(totalExecutionsCount);
    aggregations.setViewsCount(totalViewsCount);
    return aggregations;
  }

  private List<DashboardUsageAggregation> getBuckets(Filter bucketStatsFilter, String dashboardUrn) {
    AggregationSpec usersCountAggregation =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("uniqueUserCount");
    AggregationSpec viewsCountAggregation =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("viewsCount");
    AggregationSpec executionsCountAggregation =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("executionsCount");

    AggregationSpec usersCountCardinalityAggregation =
        new AggregationSpec().setAggregationType(AggregationType.CARDINALITY).setFieldPath("uniqueUserCount");
    AggregationSpec viewsCountCardinalityAggregation =
        new AggregationSpec().setAggregationType(AggregationType.CARDINALITY).setFieldPath("viewsCount");
    AggregationSpec executionsCountCardinalityAggregation =
        new AggregationSpec().setAggregationType(AggregationType.CARDINALITY).setFieldPath("executionsCount");

    AggregationSpec[] aggregationSpecs =
        new AggregationSpec[]{usersCountAggregation, viewsCountAggregation, executionsCountAggregation,
            usersCountCardinalityAggregation, viewsCountCardinalityAggregation, executionsCountCardinalityAggregation};
    GenericTable dailyStats = timeseriesAspectService.getAggregatedStats(Constants.DASHBOARD_ENTITY_NAME,
        Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME, aggregationSpecs, bucketStatsFilter,
        createUsageGroupingBuckets(CalendarInterval.DAY));
    List<DashboardUsageAggregation> buckets = new ArrayList<>();

    StringArray columnNames = dailyStats.getColumnNames();
    Integer idxTimestampMillis = columnNames.indexOf("timestampMillis");
    Integer idxUserCountSum = columnNames.indexOf("sum_uniqueUserCount");
    Integer idxViewsCountSum = columnNames.indexOf("sum_viewsCount");
    Integer idxExecutionsCountSum = columnNames.indexOf("sum_executionsCount");
    Integer idxUserCountCardinality = columnNames.indexOf("cardinality_uniqueUserCount");
    Integer idxViewsCountCardinality = columnNames.indexOf("cardinality_viewsCount");
    Integer idxExecutionsCountCardinality = columnNames.indexOf("cardinality_executionsCount");

    for (StringArray row : dailyStats.getRows()) {
      DashboardUsageAggregation usageAggregation = new DashboardUsageAggregation();
      usageAggregation.setBucket(Long.valueOf(row.get(idxTimestampMillis)));
      usageAggregation.setDuration(WindowDuration.DAY);
      usageAggregation.setResource(dashboardUrn);

      DashboardUsageAggregationMetrics usageAggregationMetrics = new DashboardUsageAggregationMetrics();

      // Note: Currently SUM AggregationType returns 0 (zero) value even if all values in timeseries field being aggregated
      // are NULL (missing). For example sum of execution counts come up as 0 if all values in executions count timeseries
      // are NULL. To overcome this, we extract CARDINALITY for the same timeseries field. Cardinality of 0 identifies
      // above scenario. For such scenario, we set sum as NULL.
      if (!row.get(idxUserCountSum).equals(ES_NULL_VALUE) && !row.get(idxUserCountCardinality).equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(idxUserCountCardinality)) != 0) {
            usageAggregationMetrics.setUniqueUserCount(Integer.valueOf(row.get(idxUserCountSum)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert uniqueUserCount from ES to int", e);
        }
      }
      if (!row.get(idxViewsCountSum).equals(ES_NULL_VALUE) && !row.get(idxViewsCountCardinality)
          .equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(idxViewsCountCardinality)) != 0) {
            usageAggregationMetrics.setViewsCount(Integer.valueOf(row.get(idxViewsCountSum)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert viewsCount from ES to int", e);
        }
      }
      if (!row.get(idxExecutionsCountSum).equals(ES_NULL_VALUE) && !row.get(idxExecutionsCountCardinality)
          .equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(idxExecutionsCountCardinality)) != 0) {
            usageAggregationMetrics.setExecutionsCount(Integer.valueOf(row.get(idxExecutionsCountSum)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert executionsCount from ES to object", e);
        }
      }
      usageAggregation.setMetrics(usageAggregationMetrics);
      buckets.add(usageAggregation);
    }
    return buckets;
  }

  private List<DashboardUserUsageCounts> getUserUsageCounts(Filter filter) {
    // Sum aggregation on userCounts.count
    AggregationSpec sumUsageCountsCountAggSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("userCounts.usageCount");
    AggregationSpec sumViewCountsCountAggSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("userCounts.viewsCount");
    AggregationSpec sumExecutionCountsCountAggSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("userCounts.executionsCount");

    AggregationSpec usageCountsCardinalityAggSpec =
        new AggregationSpec().setAggregationType(AggregationType.CARDINALITY).setFieldPath("userCounts.usageCount");
    AggregationSpec viewCountsCardinalityAggSpec =
        new AggregationSpec().setAggregationType(AggregationType.CARDINALITY).setFieldPath("userCounts.viewsCount");
    AggregationSpec executionCountsCardinalityAggSpec =
        new AggregationSpec().setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("userCounts.executionsCount");
    AggregationSpec[] aggregationSpecs =
        new AggregationSpec[]{sumUsageCountsCountAggSpec, sumViewCountsCountAggSpec, sumExecutionCountsCountAggSpec,
            usageCountsCardinalityAggSpec, viewCountsCardinalityAggSpec, executionCountsCardinalityAggSpec};

    // String grouping bucket on userCounts.user
    GroupingBucket userGroupingBucket =
        new GroupingBucket().setKey("userCounts.user").setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    GroupingBucket[] groupingBuckets = new GroupingBucket[]{userGroupingBucket};

    // Query backend
    GenericTable result = timeseriesAspectService.getAggregatedStats(Constants.DASHBOARD_ENTITY_NAME,
        Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME, aggregationSpecs, filter, groupingBuckets);

    StringArray columnNames = result.getColumnNames();

    Integer idxUser = columnNames.indexOf("userCounts.user");
    Integer idxUsageCountSum = columnNames.indexOf("sum_userCounts.usageCount");
    Integer idxViewsCountSum = columnNames.indexOf("sum_userCounts.viewsCount");
    Integer idxExecutionsCountSum = columnNames.indexOf("sum_userCounts.executionsCount");
    Integer idxUsageCountCardinality = columnNames.indexOf("cardinality_userCounts.usageCount");
    Integer idxViewsCountCardinality = columnNames.indexOf("cardinality_userCounts.viewsCount");
    Integer idxExecutionsCountCardinality = columnNames.indexOf("cardinality_userCounts.executionsCount");

    // Process response
    List<DashboardUserUsageCounts> userUsageCounts = new ArrayList<>();
    for (StringArray row : result.getRows()) {
      DashboardUserUsageCounts userUsageCount = new DashboardUserUsageCounts();

      CorpUser partialUser = new CorpUser();
      partialUser.setUrn(row.get(idxUser));
      userUsageCount.setUser(partialUser);

      // Note: Currently SUM AggregationType returns 0 (zero) value even if all values in timeseries field being aggregated
      // are NULL (missing). For example sum of execution counts come up as 0 if all values in executions count timeseries
      // are NULL. To overcome this, we extract CARDINALITY for the same timeseries field. Cardinality of 0 identifies
      // above scenario. For such scenario, we set sum as NULL.
      if (!row.get(idxUsageCountSum).equals(ES_NULL_VALUE) && !row.get(idxUsageCountCardinality)
          .equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(idxUsageCountCardinality)) != 0) {
            userUsageCount.setUsageCount(Integer.valueOf(row.get(idxUsageCountSum)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert user usage count from ES to int", e);
        }
      }
      if (!row.get(idxViewsCountSum).equals(ES_NULL_VALUE) && row.get(idxViewsCountCardinality).equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(idxViewsCountCardinality)) != 0) {
            userUsageCount.setViewsCount(Integer.valueOf(row.get(idxViewsCountSum)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert user views count from ES to int", e);
        }
      }
      if (!row.get(idxExecutionsCountSum).equals(ES_NULL_VALUE) && !row.get(idxExecutionsCountCardinality)
          .equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(idxExecutionsCountCardinality)) != 0) {
            userUsageCount.setExecutionsCount(Integer.valueOf(row.get(idxExecutionsCountSum)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert user executions count from ES to int", e);
        }
      }
      userUsageCounts.add(userUsageCount);
    }
    return userUsageCounts;
  }

  private GroupingBucket[] createUsageGroupingBuckets(CalendarInterval calenderInterval) {
    GroupingBucket timestampBucket = new GroupingBucket();
    timestampBucket.setKey(ES_FIELD_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(calenderInterval));
    return new GroupingBucket[]{timestampBucket};
  }

  private Filter createBucketUsageStatsFilter(String dashboardUrn, Long startTime, Long endTime) {
    Filter filter = new Filter();
    final ArrayList<Criterion> criteria = new ArrayList<>();

    // Add filter for urn == dashboardUrn
    Criterion dashboardUrnCriterion =
        new Criterion().setField(ES_FIELD_URN).setCondition(Condition.EQUAL).setValue(dashboardUrn);
    criteria.add(dashboardUrnCriterion);

    if (startTime != null) {
      // Add filter for start time
      Criterion startTimeCriterion = new Criterion().setField(ES_FIELD_TIMESTAMP)
          .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
          .setValue(Long.toString(startTime));
      criteria.add(startTimeCriterion);
    }

    if (endTime != null) {
      // Add filter for end time
      Criterion endTimeCriterion = new Criterion().setField(ES_FIELD_TIMESTAMP)
          .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
          .setValue(Long.toString(endTime));
      criteria.add(endTimeCriterion);
    }

    // Add filter for presence of eventGranularity - only consider bucket stats and not absolute stats
    // since unit is mandatory, we assume if eventGranularity contains unit, then it is not null
    Criterion onlyTimeBucketsCriterion =
        new Criterion().setField(ES_FIELD_EVENT_GRANULARITY).setCondition(Condition.CONTAIN).setValue("unit");
    criteria.add(onlyTimeBucketsCriterion);

    filter.setOr(new ConjunctiveCriterionArray(
        ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));
    return filter;
  }
}
