package com.linkedin.metadata.timeseries.elastic;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.IntegerMap;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.operations.OperationsAggregation;
import com.linkedin.operations.OperationsAggregationArray;
import com.linkedin.operations.OperationsAggregationsResult;
import com.linkedin.operations.OperationsQueryResult;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import com.linkedin.usage.UsageTimeRange;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OperationsServiceUtil {

  private static final String LAST_UPDATED_TIME = "lastUpdatedTimestamp";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final AggregationSpec TIMESTAMP_SPEC =
      new AggregationSpec()
          .setAggregationType(AggregationType.CARDINALITY)
          .setFieldPath(LAST_UPDATED_TIME);
  private static final GroupingBucket OPERATION_TYPE_BUCKET =
      new GroupingBucket()
          .setKey("operationType")
          .setType(GroupingBucketType.STRING_GROUPING_BUCKET);
  private static final GroupingBucket CUSTOM_OPERATION_TYPE_BUCKET =
      new GroupingBucket()
          .setKey("customOperationType")
          .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private OperationsServiceUtil() {}

  public static final String OPERATIONS_ENTITY_NAME = "dataset";
  public static final String OPERATION_ASPECT_NAME = "operation";

  public static OperationsQueryResult queryRange(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String resource,
      @Nonnull WindowDuration duration,
      UsageTimeRange range) {
    return queryRange(opContext, timeseriesAspectService, resource, duration, range, null);
  }

  public static OperationsQueryResult queryRange(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String resource,
      @Nonnull WindowDuration duration,
      UsageTimeRange range,
      @Nullable String timeZone) {

    final long now = Instant.now().toEpochMilli();
    return query(
        opContext,
        timeseriesAspectService,
        resource,
        duration,
        TimeseriesUtils.convertRangeToStartTime(range, now),
        now,
        timeZone);
  }

  public static OperationsQueryResult query(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String resource,
      @Nonnull WindowDuration duration,
      @Nullable Long startTime,
      @Nullable Long endTime,
      @Nullable String timeZone) {

    // 1. Populate the filter. This is common for all queries.
    Filter filter = new Filter();
    ArrayList<Criterion> criteria =
        TimeseriesUtils.createCommonFilterCriteria(resource, startTime, endTime);
    filter.setOr(
        new ConjunctiveCriterionArray(
            new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))));

    Timer.Context timer;
    long took;

    // 2. Get buckets of aggregations by type per day
    OperationsAggregationArray buckets =
        opContext.withSpan(
            "aggregateByDay",
            () ->
                aggregateByDay(
                    opContext, timeseriesAspectService, filter, resource, duration, timeZone),
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(OperationsServiceUtil.class, "aggregateByDay"));

    // 3. Get overall aggregations for the whole time period by operation type
    OperationsAggregationsResult aggregations =
        opContext.withSpan(
            "aggregateByType",
            () -> aggregateByType(opContext, timeseriesAspectService, filter, resource),
            MetricUtils.DROPWIZARD_NAME,
            MetricUtils.name(OperationsServiceUtil.class, "aggregateByType"));

    // 5. Populate and return the result.
    return new OperationsQueryResult().setAggregations(aggregations).setBuckets(buckets);
  }

  private static GroupingBucket getTimestampBucket(
      @Nonnull WindowDuration duration, @Nullable String timeZone) {
    return new GroupingBucket()
        .setKey(LAST_UPDATED_TIME)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeZone(timeZone, SetMode.IGNORE_NULL)
        .setTimeWindowSize(
            new TimeWindowSize()
                .setMultiple(1)
                .setUnit(TimeseriesUtils.windowToInterval(duration)));
  }

  private static OperationsAggregationArray aggregateByDay(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull Filter filter,
      @Nonnull String resource,
      @Nonnull WindowDuration duration,
      @Nullable String timeZone) {
    // Construct the Grouping buckets with ts bucket for day 1st, and the operationType 2nd
    GroupingBucket[] groupingBuckets =
        new GroupingBucket[] {getTimestampBucket(duration, timeZone), OPERATION_TYPE_BUCKET};
    AggregationSpec[] aggregationSpecs = new AggregationSpec[] {TIMESTAMP_SPEC};
    GenericTable operationsTable =
        getOperationsTable(
            opContext, timeseriesAspectService, filter, aggregationSpecs, groupingBuckets, 3);

    if (!operationsTable.hasRows()) {
      return new OperationsAggregationArray();
    }

    boolean hasCustomOperations = false;
    // Create map of timestamp ->  OperationsAggregationsResult so we can aggregate all the
    // operationTypes into one result per day
    Map<Long, OperationsAggregationsResult> timestampToAggregationsResult = new HashMap<>();
    for (StringArray row : Objects.requireNonNull(operationsTable.getRows())) {
      if (row.size() != 3) {
        log.error(
            String.format(
                "Issue getting aggregated operations data, wrong number of rows for %s", resource));
        continue;
      }
      Long timestamp = Long.valueOf(row.get(0));
      OperationsAggregationsResult aggregationsResult =
          timestampToAggregationsResult.getOrDefault(timestamp, new OperationsAggregationsResult());
      if (row.get(1).equals(Constants.ES_NULL_VALUE)
          || row.get(2).equals(Constants.ES_NULL_VALUE)) {
        continue;
      }
      String operationType = row.get(1);

      int operationCount = 0;
      try {
        operationCount = Integer.parseInt(row.get(2));
      } catch (NumberFormatException e) {
        log.error(
            String.format(
                "Found issue converting operations count from ES to int for %s", row.get(0)),
            e);
        continue;
      }

      int totalOperations =
          aggregationsResult.getTotalOperations() != null
              ? aggregationsResult.getTotalOperations()
              : 0;
      aggregationsResult.setTotalOperations(totalOperations + operationCount);
      updateOperationAggregations(operationType, operationCount, aggregationsResult);
      timestampToAggregationsResult.put(timestamp, aggregationsResult);
      if (aggregationsResult.getTotalOperations() != null
          && aggregationsResult.getTotalOperations() > 0) {
        hasCustomOperations = true;
      }
    }

    if (hasCustomOperations) {
      aggregateCustomsByDay(
          opContext,
          timeseriesAspectService,
          filter,
          resource,
          duration,
          timestampToAggregationsResult,
          timeZone);
    }

    // 5. Loop over map and create OperationsAggregation for each entry and add to
    // OperationsAggregationArray
    OperationsAggregationArray buckets = new OperationsAggregationArray();
    for (final Map.Entry<Long, OperationsAggregationsResult> entry :
        timestampToAggregationsResult.entrySet()) {
      OperationsAggregation operationAggregation = new OperationsAggregation();
      operationAggregation.setBucket(entry.getKey());
      operationAggregation.setDuration(duration);
      operationAggregation.setResource(UrnUtils.getUrn(resource));
      operationAggregation.setAggregations(entry.getValue());
      buckets.add(operationAggregation);
    }

    // 6. Return sorted list in reverse chronological order to match what the getAggregatedStats
    // give us
    // Working with the map above to get the final aggregations doesn't guarantee order
    return new OperationsAggregationArray(
        buckets.stream()
            .sorted((a, b) -> (int) (a.getBucket() - b.getBucket()))
            .collect(Collectors.toList()));
  }

  /*
   * Aggregate by customOperationType to get the map of custom types and their counts per day
   */
  private static void aggregateCustomsByDay(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull Filter filter,
      @Nonnull String resource,
      @Nonnull WindowDuration duration,
      @Nonnull Map<Long, OperationsAggregationsResult> timestampToAggregationsResult,
      @Nullable String timeZone) {
    // Construct the Grouping buckets with ts bucket for day 1st, and the customOperationType 2nd
    AggregationSpec[] aggregationSpecs = new AggregationSpec[] {TIMESTAMP_SPEC};
    GroupingBucket[] groupingBuckets =
        new GroupingBucket[] {getTimestampBucket(duration, timeZone), CUSTOM_OPERATION_TYPE_BUCKET};
    GenericTable operationsTable =
        getOperationsTable(
            opContext, timeseriesAspectService, filter, aggregationSpecs, groupingBuckets, 3);

    if (!operationsTable.hasRows()) {
      return;
    }

    // Create map of timestamp ->  OperationsAggregationsResult so we can aggregate all the
    // operationTypes into one result per day
    for (StringArray row : Objects.requireNonNull(operationsTable.getRows())) {
      if (row.size() != 3) {
        log.error(
            String.format(
                "Issue getting aggregated operations data, wrong number of rows for %s", resource));
        continue;
      }
      Long timestamp = Long.valueOf(row.get(0));
      OperationsAggregationsResult aggregationsResult =
          timestampToAggregationsResult.getOrDefault(timestamp, new OperationsAggregationsResult());
      if (row.get(1).equals(Constants.ES_NULL_VALUE)
          || row.get(2).equals(Constants.ES_NULL_VALUE)) {
        continue;
      }
      String customOperationType = row.get(1);

      int operationCount = 0;
      try {
        operationCount = Integer.parseInt(row.get(2));
      } catch (NumberFormatException e) {
        log.error(
            String.format(
                "Found issue converting operations count from ES to int for %s", row.get(0)),
            e);
        continue;
      }

      IntegerMap customOperationsMap =
          aggregationsResult.getCustomOperationsMap() != null
              ? aggregationsResult.getCustomOperationsMap()
              : new IntegerMap();
      customOperationsMap.put(customOperationType, operationCount);
      aggregationsResult.setCustomOperationsMap(customOperationsMap);
      timestampToAggregationsResult.put(timestamp, aggregationsResult);
    }
  }

  private static OperationsAggregationsResult aggregateByType(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      Filter filter,
      String resource) {
    OperationsAggregationsResult aggregations = new OperationsAggregationsResult();

    AggregationSpec[] aggregationSpecs = new AggregationSpec[] {TIMESTAMP_SPEC};
    GroupingBucket[] groupingBuckets = new GroupingBucket[] {OPERATION_TYPE_BUCKET};
    GenericTable operationsTable =
        getOperationsTable(
            opContext, timeseriesAspectService, filter, aggregationSpecs, groupingBuckets, 2);

    if (operationsTable.getRows() == null || operationsTable.getRows().size() == 0) {
      return aggregations;
    }

    int totalOperations = 0;
    for (StringArray row : operationsTable.getRows()) {
      if (row.size() != 2) {
        log.error(
            String.format(
                "Issue getting aggregated operations data, wrong number of rows for %s", resource));
        continue;
      }
      if (!row.get(0).equals(Constants.ES_NULL_VALUE)
          && !row.get(1).equals(Constants.ES_NULL_VALUE)) {
        try {
          int operationCount = Integer.parseInt(row.get(1));
          totalOperations += operationCount;
          updateOperationAggregations(row.get(0), operationCount, aggregations);
        } catch (NumberFormatException e) {
          log.error(
              String.format(
                  "Found issue converting operations count from ES to int for %s", row.get(0)),
              e);
        }
      }
    }

    if (aggregations.getTotalCustoms() != null && aggregations.getTotalCustoms() > 0) {
      aggregateCustomsByType(opContext, timeseriesAspectService, filter, resource, aggregations);
    }

    aggregations.setTotalOperations(totalOperations);

    return aggregations;
  }

  private static void aggregateCustomsByType(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      Filter filter,
      String resource,
      @Nonnull OperationsAggregationsResult currentAggregations) {
    AggregationSpec[] aggregationSpecs = new AggregationSpec[] {TIMESTAMP_SPEC};
    GroupingBucket[] groupingBuckets = new GroupingBucket[] {CUSTOM_OPERATION_TYPE_BUCKET};
    GenericTable operationsTable =
        getOperationsTable(
            opContext, timeseriesAspectService, filter, aggregationSpecs, groupingBuckets, 2);

    if (operationsTable.getRows() == null || operationsTable.getRows().size() == 0) {
      return;
    }

    IntegerMap customOperationsMap = new IntegerMap();

    for (StringArray row : operationsTable.getRows()) {
      if (row.size() != 2) {
        log.error(
            String.format(
                "Issue getting aggregated custom operations data, wrong number of rows for %s",
                resource));
        continue;
      }
      if (!row.get(0).equals(Constants.ES_NULL_VALUE)
          && !row.get(1).equals(Constants.ES_NULL_VALUE)) {
        try {
          int operationCount = Integer.parseInt(row.get(1));
          String customOperationType = row.get(0);
          customOperationsMap.put(customOperationType, operationCount);
        } catch (NumberFormatException e) {
          log.error(
              String.format(
                  "Found issue converting operations count from ES to int for %s", row.get(0)),
              e);
        }
      }
    }
    if (customOperationsMap.size() > 0) {
      currentAggregations.setCustomOperationsMap(customOperationsMap);
    }
  }

  private static GenericTable getOperationsTable(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull Filter filter,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nonnull GroupingBucket[] groupingBuckets,
      int columnCount) {
    GenericTable operationsTable =
        timeseriesAspectService.getAggregatedStats(
            opContext,
            OPERATIONS_ENTITY_NAME,
            OPERATION_ASPECT_NAME,
            aggregationSpecs,
            filter,
            groupingBuckets);

    if (operationsTable.getColumnNames().size() != columnCount) {
      throw new RuntimeException(
          "Malformed data returned when getting aggregated custom operations stats");
    }

    return operationsTable;
  }

  private static void updateOperationAggregations(
      String operationType, int operationCount, OperationsAggregationsResult aggregations) {
    switch (operationType) {
      case "INSERT":
        aggregations.setTotalInserts(operationCount);
        break;
      case "UPDATE":
        aggregations.setTotalUpdates(operationCount);
        break;
      case "DELETE":
        aggregations.setTotalDeletes(operationCount);
        break;
      case "CREATE":
        aggregations.setTotalCreates(operationCount);
        break;
      case "ALTER":
        aggregations.setTotalAlters(operationCount);
        break;
      case "DROP":
        aggregations.setTotalDrops(operationCount);
        break;
      case "CUSTOM":
        aggregations.setTotalCustoms(operationCount);
        break;
      default:
        log.debug(String.format("Found unexpected operation type %s", operationType));
    }
  }
}
