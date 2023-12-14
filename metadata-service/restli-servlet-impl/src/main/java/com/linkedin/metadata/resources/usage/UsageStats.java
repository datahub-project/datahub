package com.linkedin.metadata.resources.usage;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.resources.restli.RestliUtils.*;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetFieldUsageCounts;
import com.linkedin.dataset.DatasetFieldUsageCountsArray;
import com.linkedin.dataset.DatasetUsageStatistics;
import com.linkedin.dataset.DatasetUserUsageCounts;
import com.linkedin.dataset.DatasetUserUsageCountsArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
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
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Rest.li entry point: /usageStats */
@Slf4j
@Deprecated
@RestLiSimpleResource(name = "usageStats", namespace = "com.linkedin.usage")
public class UsageStats extends SimpleResourceTemplate<UsageAggregation> {
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

  private static final String ACTION_BATCH_INGEST = "batchIngest";
  private static final String PARAM_BUCKETS = "buckets";

  private static final String ACTION_QUERY = "query";
  private static final String PARAM_RESOURCE = "resource";
  private static final String PARAM_DURATION = "duration";
  private static final String PARAM_START_TIME = "startTime";
  private static final String PARAM_END_TIME = "endTime";
  private static final String PARAM_MAX_BUCKETS = "maxBuckets";

  private static final String ACTION_QUERY_RANGE = "queryRange";
  private static final String PARAM_RANGE = "rangeFromEnd";
  private static final String USAGE_STATS_ENTITY_NAME = "dataset";
  private static final String USAGE_STATS_ASPECT_NAME = "datasetUsageStatistics";
  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  private static final String ES_NULL_VALUE = "NULL";

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Inject
  @Named("entityRegistry")
  private EntityRegistry _entityRegistry;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

  @Getter(lazy = true)
  private final AspectSpec usageStatsAspectSpec =
      _entityRegistry.getEntitySpec(USAGE_STATS_ENTITY_NAME).getAspectSpec(USAGE_STATS_ASPECT_NAME);

  @Action(name = ACTION_BATCH_INGEST)
  @Deprecated
  @Nonnull
  @WithSpan
  public Task<Void> batchIngest(@ActionParam(PARAM_BUCKETS) @Nonnull UsageAggregation[] buckets) {
    log.info("Ingesting {} usage stats aggregations", buckets.length);
    return RestliUtil.toTask(
        () -> {
          Authentication auth = AuthenticationContext.getAuthentication();
          if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
              && !isAuthorized(
                  auth,
                  _authorizer,
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE),
                  (EntitySpec) null)) {
            throw new RestLiServiceException(
                HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to edit entities.");
          }
          for (UsageAggregation agg : buckets) {
            this.ingest(agg);
          }
          return null;
        },
        MetricRegistry.name(this.getClass(), "batchIngest"));
  }

  private CalendarInterval windowToInterval(@Nonnull WindowDuration duration) {
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

  private UsageAggregationArray getBuckets(
      @Nonnull Filter filter, @Nonnull String resource, @Nonnull WindowDuration duration) {
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
        _timeseriesAspectService.getAggregatedStats(
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

  private List<UserUsageCounts> getUserUsageCounts(Filter filter) {
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
        _timeseriesAspectService.getAggregatedStats(
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

  private List<FieldUsageCounts> getFieldUsageCounts(Filter filter) {
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
        _timeseriesAspectService.getAggregatedStats(
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

  private UsageQueryResultAggregations getAggregations(Filter filter) {
    UsageQueryResultAggregations aggregations = new UsageQueryResultAggregations();
    List<UserUsageCounts> userUsageCounts = getUserUsageCounts(filter);
    aggregations.setUsers(new UserUsageCountsArray(userUsageCounts));
    aggregations.setUniqueUserCount(userUsageCounts.size());

    List<FieldUsageCounts> fieldUsageCounts = getFieldUsageCounts(filter);
    aggregations.setFields(new FieldUsageCountsArray(fieldUsageCounts));

    return aggregations;
  }

  @Action(name = ACTION_QUERY)
  @Nonnull
  @WithSpan
  public Task<UsageQueryResult> query(
      @ActionParam(PARAM_RESOURCE) @Nonnull String resource,
      @ActionParam(PARAM_DURATION) @Nonnull WindowDuration duration,
      @ActionParam(PARAM_START_TIME) @com.linkedin.restli.server.annotations.Optional
          Long startTime,
      @ActionParam(PARAM_END_TIME) @com.linkedin.restli.server.annotations.Optional Long endTime,
      @ActionParam(PARAM_MAX_BUCKETS) @com.linkedin.restli.server.annotations.Optional
          Integer maxBuckets) {
    log.info("Attempting to query usage stats");
    return RestliUtil.toTask(
        () -> {
          Authentication auth = AuthenticationContext.getAuthentication();
          Urn resourceUrn = UrnUtils.getUrn(resource);
          if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
              && !isAuthorized(
                  auth,
                  _authorizer,
                  ImmutableList.of(PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE),
                  new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString()))) {
            throw new RestLiServiceException(
                HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to query usage.");
          }
          // 1. Populate the filter. This is common for all queries.
          Filter filter = new Filter();
          ArrayList<Criterion> criteria = new ArrayList<>();
          Criterion hasUrnCriterion =
              new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(resource);
          criteria.add(hasUrnCriterion);
          if (startTime != null) {
            Criterion startTimeCriterion =
                new Criterion()
                    .setField(ES_FIELD_TIMESTAMP)
                    .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
                    .setValue(startTime.toString());
            criteria.add(startTimeCriterion);
          }
          if (endTime != null) {
            Criterion endTimeCriterion =
                new Criterion()
                    .setField(ES_FIELD_TIMESTAMP)
                    .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
                    .setValue(endTime.toString());
            criteria.add(endTimeCriterion);
          }

          filter.setOr(
              new ConjunctiveCriterionArray(
                  new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))));

          // 2. Get buckets.
          UsageAggregationArray buckets = getBuckets(filter, resource, duration);

          // 3. Get aggregations.
          UsageQueryResultAggregations aggregations = getAggregations(filter);

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
        },
        MetricRegistry.name(this.getClass(), "query"));
  }

  @Action(name = ACTION_QUERY_RANGE)
  @Nonnull
  @WithSpan
  public Task<UsageQueryResult> queryRange(
      @ActionParam(PARAM_RESOURCE) @Nonnull String resource,
      @ActionParam(PARAM_DURATION) @Nonnull WindowDuration duration,
      @ActionParam(PARAM_RANGE) UsageTimeRange range) {
    Authentication auth = AuthenticationContext.getAuthentication();
    Urn resourceUrn = UrnUtils.getUrn(resource);
    if (Boolean.parseBoolean(System.getenv(REST_API_AUTHORIZATION_ENABLED_ENV))
        && !isAuthorized(
            auth,
            _authorizer,
            ImmutableList.of(PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE),
            new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString()))) {
      throw new RestLiServiceException(
          HttpStatus.S_401_UNAUTHORIZED, "User is unauthorized to query usage.");
    }
    final long now = Instant.now().toEpochMilli();
    return this.query(resource, duration, convertRangeToStartTime(range, now), now, null);
  }

  private void ingest(@Nonnull UsageAggregation bucket) {
    // 1. Translate the bucket to DatasetUsageStatistics first.
    DatasetUsageStatistics datasetUsageStatistics = new DatasetUsageStatistics();
    datasetUsageStatistics.setTimestampMillis(bucket.getBucket());
    datasetUsageStatistics.setEventGranularity(
        new TimeWindowSize().setUnit(windowToInterval(bucket.getDuration())).setMultiple(1));
    UsageAggregationMetrics aggregationMetrics = bucket.getMetrics();
    if (aggregationMetrics.hasUniqueUserCount()) {
      datasetUsageStatistics.setUniqueUserCount(aggregationMetrics.getUniqueUserCount());
    }
    if (aggregationMetrics.hasTotalSqlQueries()) {
      datasetUsageStatistics.setTotalSqlQueries(aggregationMetrics.getTotalSqlQueries());
    }
    if (aggregationMetrics.hasTopSqlQueries()) {
      datasetUsageStatistics.setTopSqlQueries(aggregationMetrics.getTopSqlQueries());
    }
    if (aggregationMetrics.hasUsers()) {
      DatasetUserUsageCountsArray datasetUserUsageCountsArray = new DatasetUserUsageCountsArray();
      for (UserUsageCounts u : aggregationMetrics.getUsers()) {
        DatasetUserUsageCounts datasetUserUsageCounts = new DatasetUserUsageCounts();
        datasetUserUsageCounts.setUser(u.getUser());
        datasetUserUsageCounts.setCount(u.getCount());
        datasetUserUsageCountsArray.add(datasetUserUsageCounts);
      }
      datasetUsageStatistics.setUserCounts(datasetUserUsageCountsArray);
    }
    if (aggregationMetrics.hasFields()) {
      DatasetFieldUsageCountsArray datasetFieldUsageCountsArray =
          new DatasetFieldUsageCountsArray();
      for (FieldUsageCounts f : aggregationMetrics.getFields()) {
        DatasetFieldUsageCounts datasetFieldUsageCounts = new DatasetFieldUsageCounts();
        datasetFieldUsageCounts.setFieldPath(f.getFieldName());
        datasetFieldUsageCounts.setCount(f.getCount());
        datasetFieldUsageCountsArray.add(datasetFieldUsageCounts);
      }
      datasetUsageStatistics.setFieldCounts(datasetFieldUsageCountsArray);
    }
    // 2. Transform the aspect to timeseries documents.
    Map<String, JsonNode> documents;
    try {
      documents =
          TimeseriesAspectTransformer.transform(
              bucket.getResource(), datasetUsageStatistics, getUsageStatsAspectSpec(), null);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate timeseries document from aspect: {}", e.toString());
      return;
    }
    // 3. Upsert the exploded documents to timeseries aspect service.
    documents
        .entrySet()
        .forEach(
            document -> {
              _timeseriesAspectService.upsertDocument(
                  USAGE_STATS_ENTITY_NAME,
                  USAGE_STATS_ASPECT_NAME,
                  document.getKey(),
                  document.getValue());
            });
  }

  @Nonnull
  Long convertRangeToStartTime(@Nonnull UsageTimeRange range, long currentEpochMillis) {
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
