package com.linkedin.metadata.timeseries.elastic;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import com.linkedin.usage.FieldUsageCounts;
import com.linkedin.usage.FieldUsageCountsArray;
import com.linkedin.usage.UsageAggregation;
import com.linkedin.usage.UsageAggregationArray;
import com.linkedin.usage.UsageAggregationMetrics;
import com.linkedin.usage.UsageQueryResult;
import com.linkedin.usage.UsageQueryResultAggregations;
import com.linkedin.usage.UsageTimeRange;
import com.linkedin.usage.UserUsageCounts;
import com.linkedin.usage.UserUsageCountsArray;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UsageServiceUtil {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private UsageServiceUtil() {}

  public static final String USAGE_STATS_ENTITY_NAME = "dataset";
  public static final String USAGE_STATS_ASPECT_NAME = "datasetUsageStatistics";
  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  public static final String ES_NULL_VALUE = "NULL";

  public static UsageQueryResult queryRange(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String resource,
      @Nonnull WindowDuration duration,
      UsageTimeRange range) {

    final long now = Instant.now().toEpochMilli();
    return query(
        opContext,
        timeseriesAspectService,
        resource,
        duration,
        convertRangeToStartTime(range, now),
        now,
        null);
  }

  public static UsageQueryResult query(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String resource,
      @Nonnull WindowDuration duration,
      @Nullable Long startTime,
      @Nullable Long endTime,
      @Nullable Integer maxBuckets) {

    // 1. Populate the filter. This is common for all queries.
    Filter filter = new Filter();
    ArrayList<Criterion> criteria = new ArrayList<>();
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, resource);

    criteria.add(hasUrnCriterion);
    if (startTime != null) {
      Criterion startTimeCriterion =
          buildCriterion(
              ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());

      criteria.add(startTimeCriterion);
    }
    if (endTime != null) {
      Criterion endTimeCriterion =
          buildCriterion(ES_FIELD_TIMESTAMP, Condition.LESS_THAN_OR_EQUAL_TO, endTime.toString());
      criteria.add(endTimeCriterion);
    }

    filter.setOr(
        new ConjunctiveCriterionArray(
            new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))));

    Timer.Context timer;
    long took;

    // 2. Get buckets.
    UsageAggregationArray buckets =
        opContext.withSpan(
            "getBuckets",
            () -> getBuckets(opContext, timeseriesAspectService, filter, resource, duration),
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(UsageServiceUtil.class, "getBuckets"));
    log.info("Usage stats for resource {} returned {} buckets", resource, buckets.size());

    // 3. Get aggregations.
    UsageQueryResultAggregations aggregations =
        opContext.withSpan(
            "getAggregations",
            () -> getAggregations(opContext, timeseriesAspectService, filter),
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(UsageServiceUtil.class, "getAggregations"));
    log.info("Usage stats aggregation for resource {}", resource);

    // 4. Compute totalSqlQuery count from the buckets itself.
    // We want to avoid issuing an additional query with a sum aggregation.
    Integer totalQueryCount = null;
    for (UsageAggregation bucket : buckets) {
      if (bucket.getMetrics().getTotalSqlQueries() != null) {
        if (totalQueryCount == null) {
          totalQueryCount = 0;
        }
        totalQueryCount += bucket.getMetrics().getTotalSqlQueries();
      }
    }

    if (totalQueryCount != null) {
      aggregations.setTotalSqlQueries(totalQueryCount);
    }

    // 5. Populate and return the result.
    return new UsageQueryResult().setBuckets(buckets).setAggregations(aggregations);
  }

  private static UsageAggregationArray getBuckets(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull Filter filter,
      @Nonnull String resource,
      @Nonnull WindowDuration duration) {
    // NOTE: We will not populate the per-bucket userCounts and fieldCounts in this implementation
    // because
    // (a) it is very expensive to compute the un-explode equivalent queries for timeseries field
    // collections, and
    // (b) the equivalent data for the whole query will anyways be populated in the `aggregations`
    // part of the results
    // (see getAggregations).

    // 1. Construct the aggregation specs for latest value of uniqueUserCount, totalSqlQueries &
    // topSqlQueries.
    AggregationSpec uniqueUserCountAgg =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("uniqueUserCount");
    AggregationSpec totalSqlQueriesAgg =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("totalSqlQueries");
    AggregationSpec topSqlQueriesAgg =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("topSqlQueries");
    AggregationSpec[] aggregationSpecs =
        new AggregationSpec[] {uniqueUserCountAgg, totalSqlQueriesAgg, topSqlQueriesAgg};

    // 2. Construct the Grouping buckets with just the ts bucket.

    GroupingBucket timestampBucket = new GroupingBucket();
    timestampBucket
        .setKey(ES_FIELD_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(windowToInterval(duration)));
    GroupingBucket[] groupingBuckets = new GroupingBucket[] {timestampBucket};

    // 3. Query
    GenericTable result =
        timeseriesAspectService.getAggregatedStats(
            opContext,
            USAGE_STATS_ENTITY_NAME,
            USAGE_STATS_ASPECT_NAME,
            aggregationSpecs,
            filter,
            groupingBuckets);

    // 4. Populate buckets from the result.
    UsageAggregationArray buckets = new UsageAggregationArray();
    for (StringArray row : result.getRows()) {
      UsageAggregation usageAggregation = new UsageAggregation();
      usageAggregation.setBucket(Long.valueOf(row.get(0)));
      usageAggregation.setDuration(duration);
      try {
        usageAggregation.setResource(new Urn(resource));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Invalid resource", e);
      }
      UsageAggregationMetrics usageAggregationMetrics = new UsageAggregationMetrics();
      if (!row.get(1).equals(ES_NULL_VALUE)) {
        try {
          usageAggregationMetrics.setUniqueUserCount(Integer.valueOf(row.get(1)));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert uniqueUserCount from ES to int", e);
        }
      }
      if (!row.get(2).equals(ES_NULL_VALUE)) {
        try {
          usageAggregationMetrics.setTotalSqlQueries(Integer.valueOf(row.get(2)));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to convert totalSqlQueries from ES to int", e);
        }
      }
      if (!row.get(3).equals(ES_NULL_VALUE)) {
        try {
          usageAggregationMetrics.setTopSqlQueries(
              OBJECT_MAPPER.readValue(row.get(3), StringArray.class));
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException(
              "Failed to convert topSqlQueries from ES to object", e);
        }
      }
      usageAggregation.setMetrics(usageAggregationMetrics);
      buckets.add(usageAggregation);
    }

    return buckets;
  }

  private static UsageQueryResultAggregations getAggregations(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      Filter filter) {
    UsageQueryResultAggregations aggregations = new UsageQueryResultAggregations();
    List<UserUsageCounts> userUsageCounts =
        getUserUsageCounts(opContext, timeseriesAspectService, filter);
    aggregations.setUsers(new UserUsageCountsArray(userUsageCounts));
    aggregations.setUniqueUserCount(userUsageCounts.size());

    List<FieldUsageCounts> fieldUsageCounts =
        getFieldUsageCounts(opContext, timeseriesAspectService, filter);
    aggregations.setFields(new FieldUsageCountsArray(fieldUsageCounts));

    return aggregations;
  }

  private static List<UserUsageCounts> getUserUsageCounts(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      Filter filter) {
    // Sum aggregation on userCounts.count
    AggregationSpec sumUserCountsCountAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("userCounts.count");
    AggregationSpec latestUserEmailAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("userCounts.userEmail");
    AggregationSpec[] aggregationSpecs =
        new AggregationSpec[] {sumUserCountsCountAggSpec, latestUserEmailAggSpec};

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
            USAGE_STATS_ENTITY_NAME,
            USAGE_STATS_ASPECT_NAME,
            aggregationSpecs,
            filter,
            groupingBuckets);
    // Process response
    List<UserUsageCounts> userUsageCounts = new ArrayList<>();
    for (StringArray row : result.getRows()) {
      UserUsageCounts userUsageCount = new UserUsageCounts();
      try {
        userUsageCount.setUser(new Urn(row.get(0)));
      } catch (URISyntaxException e) {
        log.error("Failed to convert {} to urn. Exception: {}", row.get(0), e);
      }
      if (!row.get(1).equals(ES_NULL_VALUE)) {
        try {
          userUsageCount.setCount(Integer.valueOf(row.get(1)));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Failed to convert user usage count from ES to int", e);
        }
      }
      if (!row.get(2).equals(ES_NULL_VALUE)) {
        userUsageCount.setUserEmail(row.get(2));
      }
      userUsageCounts.add(userUsageCount);
    }
    return userUsageCounts;
  }

  private static List<FieldUsageCounts> getFieldUsageCounts(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      Filter filter) {
    // Sum aggregation on fieldCounts.count
    AggregationSpec sumFieldCountAggSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("fieldCounts.count");
    AggregationSpec[] aggregationSpecs = new AggregationSpec[] {sumFieldCountAggSpec};

    // String grouping bucket on fieldCounts.fieldName
    GroupingBucket userGroupingBucket =
        new GroupingBucket()
            .setKey("fieldCounts.fieldPath")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    GroupingBucket[] groupingBuckets = new GroupingBucket[] {userGroupingBucket};

    // Query backend
    GenericTable result =
        timeseriesAspectService.getAggregatedStats(
            opContext,
            USAGE_STATS_ENTITY_NAME,
            USAGE_STATS_ASPECT_NAME,
            aggregationSpecs,
            filter,
            groupingBuckets);

    // Process response
    List<FieldUsageCounts> fieldUsageCounts = new ArrayList<>();
    for (StringArray row : result.getRows()) {
      FieldUsageCounts fieldUsageCount = new FieldUsageCounts();
      fieldUsageCount.setFieldName(row.get(0));
      if (!row.get(1).equals(ES_NULL_VALUE)) {
        try {
          fieldUsageCount.setCount(Integer.valueOf(row.get(1)));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Failed to convert field usage count from ES to int", e);
        }
      }
      fieldUsageCounts.add(fieldUsageCount);
    }
    return fieldUsageCounts;
  }

  public static CalendarInterval windowToInterval(@Nonnull WindowDuration duration) {
    switch (duration) {
      case HOUR:
        return CalendarInterval.HOUR;
      case DAY:
        return CalendarInterval.DAY;
      case WEEK:
        return CalendarInterval.WEEK;
      case MONTH:
        return CalendarInterval.MONTH;
      case YEAR:
        return CalendarInterval.YEAR;
      default:
        throw new IllegalArgumentException("Unsupported duration value" + duration);
    }
  }

  @Nonnull
  private static Long convertRangeToStartTime(
      @Nonnull UsageTimeRange range, long currentEpochMillis) {
    // TRICKY: since start_time must be before the bucket's start, we actually
    // need to subtract extra from the current time to ensure that we get precisely
    // what we're looking for. Note that start_time and end_time are both inclusive,
    // so we must also do an off-by-one adjustment.
    final long oneHourMillis = 60 * 60 * 1000;
    final long oneDayMillis = 24 * oneHourMillis;

    if (range == UsageTimeRange.HOUR) {
      return currentEpochMillis - (2 * oneHourMillis + 1);
    } else if (range == UsageTimeRange.DAY) {
      return currentEpochMillis - (2 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.WEEK) {
      return currentEpochMillis - (8 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.MONTH) {
      // Assuming month is last 30 days.
      return currentEpochMillis - (31 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.QUARTER) {
      // Assuming a quarter is 91 days.
      return currentEpochMillis - (92 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.YEAR) {
      return currentEpochMillis - (366 * oneDayMillis + 1);
    } else if (range == UsageTimeRange.ALL) {
      return 0L;
    } else {
      throw new IllegalArgumentException("invalid UsageTimeRange enum state: " + range.name());
    }
  }
}
