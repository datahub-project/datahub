package com.linkedin.datahub.graphql.resolvers.dashboard;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregation;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregationMetrics;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.generated.DashboardUsageQueryResultAggregations;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DashboardUsageStatsUtils {

  public static final String ES_FIELD_URN = "urn";
  public static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  public static final String ES_FIELD_EVENT_GRANULARITY = "eventGranularity";
  public static final String ES_NULL_VALUE = "NULL";

  public static List<DashboardUsageMetrics> getDashboardUsageMetrics(
      String dashboardUrn,
      Long maybeStartTimeMillis,
      Long maybeEndTimeMillis,
      Integer maybeLimit,
      TimeseriesAspectService timeseriesAspectService) {
    List<DashboardUsageMetrics> dashboardUsageMetrics;
    try {
      Filter filter = createUsageFilter(dashboardUrn, null, null, false);
      List<EnvelopedAspect> aspects =
          timeseriesAspectService.getAspectValues(
              Urn.createFromString(dashboardUrn),
              Constants.DASHBOARD_ENTITY_NAME,
              Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
              maybeStartTimeMillis,
              maybeEndTimeMillis,
              maybeLimit,
              filter);
      dashboardUsageMetrics =
          aspects.stream().map(DashboardUsageMetricMapper::map).collect(Collectors.toList());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid resource", e);
    }
    return dashboardUsageMetrics;
  }

  public static DashboardUsageQueryResultAggregations getAggregations(
      Filter filter,
      List<DashboardUsageAggregation> dailyUsageBuckets,
      TimeseriesAspectService timeseriesAspectService) {

    List<DashboardUserUsageCounts> userUsageCounts =
        getUserUsageCounts(filter, timeseriesAspectService);
    DashboardUsageQueryResultAggregations aggregations =
        new DashboardUsageQueryResultAggregations();
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

  public static List<DashboardUsageAggregation> getBuckets(
      Filter filter, String dashboardUrn, TimeseriesAspectService timeseriesAspectService) {
    AggregationSpec usersCountAggregation =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("uniqueUserCount");
    AggregationSpec viewsCountAggregation =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("viewsCount");
    AggregationSpec executionsCountAggregation =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("executionsCount");

    AggregationSpec usersCountCardinalityAggregation =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("uniqueUserCount");
    AggregationSpec viewsCountCardinalityAggregation =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("viewsCount");
    AggregationSpec executionsCountCardinalityAggregation =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("executionsCount");

    AggregationSpec[] aggregationSpecs =
        new AggregationSpec[] {
          usersCountAggregation,
          viewsCountAggregation,
          executionsCountAggregation,
          usersCountCardinalityAggregation,
          viewsCountCardinalityAggregation,
          executionsCountCardinalityAggregation
        };
    GenericTable dailyStats =
        timeseriesAspectService.getAggregatedStats(
            Constants.DASHBOARD_ENTITY_NAME,
            Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
            aggregationSpecs,
            filter,
            createUsageGroupingBuckets(CalendarInterval.DAY));
    List<DashboardUsageAggregation> buckets = new ArrayList<>();

    for (StringArray row : dailyStats.getRows()) {
      DashboardUsageAggregation usageAggregation = new DashboardUsageAggregation();
      usageAggregation.setBucket(Long.valueOf(row.get(0)));
      usageAggregation.setDuration(WindowDuration.DAY);
      usageAggregation.setResource(dashboardUrn);

      DashboardUsageAggregationMetrics usageAggregationMetrics =
          new DashboardUsageAggregationMetrics();

      if (!row.get(1).equals(ES_NULL_VALUE) && !row.get(4).equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(4)) != 0) {
            usageAggregationMetrics.setUniqueUserCount(Integer.valueOf(row.get(1)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert uniqueUserCount from ES to int", e);
        }
      }
      if (!row.get(2).equals(ES_NULL_VALUE) && !row.get(5).equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(5)) != 0) {
            usageAggregationMetrics.setViewsCount(Integer.valueOf(row.get(2)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert viewsCount from ES to int", e);
        }
      }
      if (!row.get(3).equals(ES_NULL_VALUE) && !row.get(5).equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(6)) != 0) {
            usageAggregationMetrics.setExecutionsCount(Integer.valueOf(row.get(3)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Failed to convert executionsCount from ES to object", e);
        }
      }
      usageAggregation.setMetrics(usageAggregationMetrics);
      buckets.add(usageAggregation);
    }
    return buckets;
  }

  public static List<DashboardUserUsageCounts> getUserUsageCounts(
      Filter filter, TimeseriesAspectService timeseriesAspectService) {
    // Sum aggregation on userCounts.count
    AggregationSpec sumUsageCountsCountAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("userCounts.usageCount");
    AggregationSpec sumViewCountsCountAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("userCounts.viewsCount");
    AggregationSpec sumExecutionCountsCountAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("userCounts.executionsCount");

    AggregationSpec usageCountsCardinalityAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("userCounts.usageCount");
    AggregationSpec viewCountsCardinalityAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("userCounts.viewsCount");
    AggregationSpec executionCountsCardinalityAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("userCounts.executionsCount");
    AggregationSpec[] aggregationSpecs =
        new AggregationSpec[] {
          sumUsageCountsCountAggSpec,
          sumViewCountsCountAggSpec,
          sumExecutionCountsCountAggSpec,
          usageCountsCardinalityAggSpec,
          viewCountsCardinalityAggSpec,
          executionCountsCardinalityAggSpec
        };

    // String grouping bucket on userCounts.user
    GroupingBucket userGroupingBucket =
        new GroupingBucket()
            .setKey("userCounts.user")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    GroupingBucket[] groupingBuckets = new GroupingBucket[] {userGroupingBucket};

    // Query backend
    GenericTable result =
        timeseriesAspectService.getAggregatedStats(
            Constants.DASHBOARD_ENTITY_NAME,
            Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
            aggregationSpecs,
            filter,
            groupingBuckets);
    // Process response
    List<DashboardUserUsageCounts> userUsageCounts = new ArrayList<>();
    for (StringArray row : result.getRows()) {
      DashboardUserUsageCounts userUsageCount = new DashboardUserUsageCounts();

      CorpUser partialUser = new CorpUser();
      partialUser.setUrn(row.get(0));
      userUsageCount.setUser(partialUser);

      if (!row.get(1).equals(ES_NULL_VALUE) && !row.get(4).equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(4)) != 0) {
            userUsageCount.setUsageCount(Integer.valueOf(row.get(1)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Failed to convert user usage count from ES to int", e);
        }
      }
      if (!row.get(2).equals(ES_NULL_VALUE) && row.get(5).equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(5)) != 0) {
            userUsageCount.setViewsCount(Integer.valueOf(row.get(2)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Failed to convert user views count from ES to int", e);
        }
      }
      if (!row.get(3).equals(ES_NULL_VALUE) && !row.get(6).equals(ES_NULL_VALUE)) {
        try {
          if (Integer.valueOf(row.get(6)) != 0) {
            userUsageCount.setExecutionsCount(Integer.valueOf(row.get(3)));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Failed to convert user executions count from ES to int", e);
        }
      }
      userUsageCounts.add(userUsageCount);
    }

    // Sort in descending order
    userUsageCounts.sort((a, b) -> (b.getUsageCount() - a.getUsageCount()));
    return userUsageCounts;
  }

  private static GroupingBucket[] createUsageGroupingBuckets(CalendarInterval calenderInterval) {
    GroupingBucket timestampBucket = new GroupingBucket();
    timestampBucket
        .setKey(ES_FIELD_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(calenderInterval));
    return new GroupingBucket[] {timestampBucket};
  }

  public static Filter createUsageFilter(
      String dashboardUrn, Long startTime, Long endTime, boolean byBucket) {
    Filter filter = new Filter();
    final ArrayList<Criterion> criteria = new ArrayList<>();

    // Add filter for urn == dashboardUrn
    Criterion dashboardUrnCriterion =
        new Criterion().setField(ES_FIELD_URN).setCondition(Condition.EQUAL).setValue(dashboardUrn);
    criteria.add(dashboardUrnCriterion);

    if (startTime != null) {
      // Add filter for start time
      Criterion startTimeCriterion =
          new Criterion()
              .setField(ES_FIELD_TIMESTAMP)
              .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
              .setValue(Long.toString(startTime));
      criteria.add(startTimeCriterion);
    }

    if (endTime != null) {
      // Add filter for end time
      Criterion endTimeCriterion =
          new Criterion()
              .setField(ES_FIELD_TIMESTAMP)
              .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
              .setValue(Long.toString(endTime));
      criteria.add(endTimeCriterion);
    }

    if (byBucket) {
      // Add filter for presence of eventGranularity - only consider bucket stats and not absolute
      // stats
      // since unit is mandatory, we assume if eventGranularity contains unit, then it is not null
      Criterion onlyTimeBucketsCriterion =
          new Criterion()
              .setField(ES_FIELD_EVENT_GRANULARITY)
              .setCondition(Condition.CONTAIN)
              .setValue("unit");
      criteria.add(onlyTimeBucketsCriterion);
    } else {
      // Add filter for absence of eventGranularity - only consider absolute stats
      Criterion excludeTimeBucketsCriterion =
          new Criterion()
              .setField(ES_FIELD_EVENT_GRANULARITY)
              .setCondition(Condition.IS_NULL)
              .setValue("");
      criteria.add(excludeTimeBucketsCriterion);
    }

    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));
    return filter;
  }

  public static Long timeMinusOneMonth(long time) {
    final long oneHourMillis = 60 * 60 * 1000;
    final long oneDayMillis = 24 * oneHourMillis;
    return time - (31 * oneDayMillis + 1);
  }

  private DashboardUsageStatsUtils() {}
}
