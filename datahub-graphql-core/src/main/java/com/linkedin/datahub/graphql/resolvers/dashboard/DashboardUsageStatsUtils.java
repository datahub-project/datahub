package com.linkedin.datahub.graphql.resolvers.dashboard;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
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
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DashboardUsageStatsUtils {

  public static final String ES_FIELD_URN = "urn";
  public static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  public static final String ES_FIELD_EVENT_GRANULARITY = "eventGranularity";
  public static final String ES_NULL_VALUE = "NULL";

  public static List<DashboardUsageMetrics> getDashboardUsageMetrics(
      @Nullable QueryContext context,
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
              context.getOperationContext(),
              Urn.createFromString(dashboardUrn),
              Constants.DASHBOARD_ENTITY_NAME,
              Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
              maybeStartTimeMillis,
              maybeEndTimeMillis,
              maybeLimit,
              filter);
      dashboardUsageMetrics =
          aspects.stream()
              .map(m -> DashboardUsageMetricMapper.map(context, m))
              .collect(Collectors.toList());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid resource", e);
    }
    return dashboardUsageMetrics;
  }

  public static DashboardUsageQueryResultAggregations getAggregations(
      @Nonnull OperationContext opContext,
      Filter filter,
      List<DashboardUsageAggregation> dailyUsageBuckets,
      TimeseriesAspectService timeseriesAspectService) {

    List<DashboardUserUsageCounts> userUsageCounts =
        getUserUsageCounts(opContext, filter, timeseriesAspectService);
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
      @Nonnull OperationContext opContext,
      Filter filter,
      String dashboardUrn,
      TimeseriesAspectService timeseriesAspectService) {
    GenericTable dailyStats =
        timeseriesAspectService.getAggregatedStats(
            opContext,
            Constants.DASHBOARD_ENTITY_NAME,
            Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
            getUsageBucketAggSpecs(),
            filter,
            getDailyUsageGroupingBuckets());
    return mapUsageAggregations(dailyStats, dashboardUrn);
  }

  /**
   * The SUM + CARDINALITY aggregation specs for the daily-usage time buckets. Column order is load
   * bearing: {@link #mapUsageAggregations} reads SUM values at rows 1-3 and CARDINALITY guards at
   * rows 4-6.
   */
  public static AggregationSpec[] getUsageBucketAggSpecs() {
    return new AggregationSpec[] {
      new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("uniqueUserCount"),
      new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("viewsCount"),
      new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("executionsCount"),
      new AggregationSpec()
          .setAggregationType(AggregationType.CARDINALITY)
          .setFieldPath("uniqueUserCount"),
      new AggregationSpec()
          .setAggregationType(AggregationType.CARDINALITY)
          .setFieldPath("viewsCount"),
      new AggregationSpec()
          .setAggregationType(AggregationType.CARDINALITY)
          .setFieldPath("executionsCount")
    };
  }

  /** Daily ({@code CalendarInterval.DAY}) date-histogram grouping for the usage time buckets. */
  public static GroupingBucket[] getDailyUsageGroupingBuckets() {
    return createUsageGroupingBuckets(CalendarInterval.DAY);
  }

  /**
   * Maps one dashboard's daily-usage aggregation table (from either {@code getAggregatedStats} or
   * the per-URN slice of {@code batchGetAggregatedStats}) into usage aggregation buckets. Assumes
   * the column layout produced by {@link #getUsageBucketAggSpecs} under a daily date-histogram.
   */
  public static List<DashboardUsageAggregation> mapUsageAggregations(
      @Nonnull GenericTable dailyStats, String dashboardUrn) {
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
      @Nonnull OperationContext opContext,
      Filter filter,
      TimeseriesAspectService timeseriesAspectService) {
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
            opContext,
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
      if (!row.get(2).equals(ES_NULL_VALUE) && !row.get(5).equals(ES_NULL_VALUE)) {
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
    userUsageCounts.sort(
        (a, b) ->
            (Objects.requireNonNullElse(b.getUsageCount(), 0)
                - Objects.requireNonNullElse(a.getUsageCount(), 0)));
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

  /** Builds a per-URN usage filter; see {@link #buildUsageFilter} for shared logic. */
  public static Filter createUsageFilter(
      String dashboardUrn, Long startTime, Long endTime, boolean byBucket) {
    return buildUsageFilter(dashboardUrn, startTime, endTime, byBucket);
  }

  public static Long timeMinusOneMonth(long time) {
    final long oneHourMillis = 60 * 60 * 1000;
    final long oneDayMillis = 24 * oneHourMillis;
    return time - (31 * oneDayMillis + 1);
  }

  /**
   * Builds a usage filter without a per-URN criterion; see {@link #buildUsageFilter} for shared
   * logic. Use this when calling batch APIs that add URN filtering internally.
   */
  public static Filter buildSharedUsageFilter(
      @Nullable Long startTime, @Nullable Long endTime, boolean byBucket) {
    return buildUsageFilter(null, startTime, endTime, byBucket);
  }

  private static Filter buildUsageFilter(
      @Nullable String dashboardUrn,
      @Nullable Long startTime,
      @Nullable Long endTime,
      boolean byBucket) {
    final ArrayList<Criterion> criteria = new ArrayList<>();

    if (dashboardUrn != null) {
      // Add filter for urn == dashboardUrn
      criteria.add(buildCriterion(ES_FIELD_URN, Condition.EQUAL, dashboardUrn));
    }

    if (startTime != null) {
      // Add filter for start time
      criteria.add(
          buildCriterion(
              ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, Long.toString(startTime)));
    }

    if (endTime != null) {
      // Add filter for end time
      criteria.add(
          buildCriterion(
              ES_FIELD_TIMESTAMP, Condition.LESS_THAN_OR_EQUAL_TO, Long.toString(endTime)));
    }

    if (byBucket) {
      // Add filter for presence of eventGranularity - only consider bucket stats and not absolute
      // stats
      // since unit is mandatory, we assume if eventGranularity contains unit, then it is not null
      criteria.add(buildCriterion(ES_FIELD_EVENT_GRANULARITY, Condition.CONTAIN, "unit"));
    } else {
      // Add filter for absence of eventGranularity - only consider absolute stats
      criteria.add(buildIsNullCriterion(ES_FIELD_EVENT_GRANULARITY));
    }

    Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));
    return filter;
  }

  /** Returns aggregation specs for counting distinct users via HyperLogLog cardinality. */
  public static AggregationSpec[] getUniqueUserCountAggSpecs() {
    return new AggregationSpec[] {
      new AggregationSpec()
          .setAggregationType(AggregationType.CARDINALITY)
          .setFieldPath("userCounts.user")
    };
  }

  /**
   * Returns a STRING grouping bucket for {@code userCounts.user} capped at {@code size}, ordered by
   * the first aggregation spec descending. ES returns exactly {@code size} buckets already ranked
   * by metric — no client-side sort needed.
   */
  public static GroupingBucket[] getTopUsersGroupingBuckets(int size) {
    return new GroupingBucket[] {
      new GroupingBucket()
          .setKey("userCounts.user")
          .setType(GroupingBucketType.STRING_GROUPING_BUCKET)
          .setSize(size)
          .setOrderByMetric(true)
          .setAscending(false)
    };
  }

  /** Returns aggregation specs for summing per-user usage counts (used for top-N ordering). */
  public static AggregationSpec[] getTopUsersSumAggSpecs() {
    return new AggregationSpec[] {
      new AggregationSpec()
          .setAggregationType(AggregationType.SUM)
          .setFieldPath("userCounts.usageCount")
    };
  }

  /**
   * Parses a {@link GenericTable} from a STRING-grouped top-users batch aggregation result into a
   * list of {@link CorpUser}s. Expected column 0: userCounts.user URN. ES returns rows already
   * ordered by metric descending (via {@code orderByMetric=true}), so no client-side sort is
   * needed.
   */
  public static List<CorpUser> buildTopUsersFromBatchAggResult(@Nonnull GenericTable table) {
    return table.getRows().stream()
        .filter(row -> row.size() > 0 && !ES_NULL_VALUE.equals(row.get(0)))
        .map(
            row -> {
              CorpUser user = new CorpUser();
              user.setUrn(row.get(0));
              return user;
            })
        .collect(Collectors.toList());
  }

  private static long parseCountOrZero(@Nullable String value) {
    if (value == null || ES_NULL_VALUE.equals(value)) {
      return 0L;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  private DashboardUsageStatsUtils() {}

  /** Maps raw timeseries documents (e.g. from the batch loader) into dashboard usage metrics. */
  public static List<DashboardUsageMetrics> mapDashboardUsageMetrics(
      @Nullable QueryContext context, @Nonnull List<EnvelopedAspect> aspects) {
    return aspects.stream()
        .map(m -> DashboardUsageMetricMapper.map(context, m))
        .collect(Collectors.toList());
  }
}
