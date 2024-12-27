package com.linkedin.metadata.timeseries.elastic;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.UrnUtils;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OperationsServiceUtil {

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

  private OperationsServiceUtil() {}

  public static final String OPERATIONS_ENTITY_NAME = "dataset";
  public static final String OPERATION_ASPECT_NAME = "operation";

  public static OperationsQueryResult queryRange(
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
        TimeseriesUtils.convertRangeToStartTime(range, now),
        now);
  }

  public static OperationsQueryResult query(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String resource,
      @Nonnull WindowDuration duration,
      @Nullable Long startTime,
      @Nullable Long endTime) {

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
    timer = MetricUtils.timer(OperationsServiceUtil.class, "aggregateByDay").time();
    OperationsAggregationArray buckets =
        aggregateByDay(opContext, timeseriesAspectService, filter, resource, duration);
    took = timer.stop();
    log.info(
        "Operations for resource {} returned {} buckets in {} ms",
        resource,
        buckets.size(),
        TimeUnit.NANOSECONDS.toMillis(took));

    // 3. Get overall aggregations for the whole time period by operation type
    timer = MetricUtils.timer(OperationsServiceUtil.class, "aggregateByType").time();
    OperationsAggregationsResult aggregations =
        aggregateByType(opContext, timeseriesAspectService, filter, resource);
    took = timer.stop();
    log.info(
        "Operations aggregation for resource {} took {} ms",
        resource,
        TimeUnit.NANOSECONDS.toMillis(took));

    // 5. Populate and return the result.
    return new OperationsQueryResult().setAggregations(aggregations).setBuckets(buckets);
  }

  private static OperationsAggregationArray aggregateByDay(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull Filter filter,
      @Nonnull String resource,
      @Nonnull WindowDuration duration) {
    // 1. Construct the aggregation specs for count of items per operation type per day
    AggregationSpec timestampSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath(Constants.ES_FIELD_TIMESTAMP);
    AggregationSpec[] aggregationSpecs = new AggregationSpec[] {timestampSpec};

    // 2. Construct the Grouping buckets with the ts bucket for day first, and the operationType as
    // a secondary bucket
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(Constants.ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(
                new TimeWindowSize()
                    .setMultiple(1)
                    .setUnit(TimeseriesUtils.windowToInterval(duration)));
    GroupingBucket operationTypeBucket =
        new GroupingBucket()
            .setKey("operationType")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GroupingBucket[] groupingBuckets = new GroupingBucket[] {timestampBucket, operationTypeBucket};

    // 3. Query
    GenericTable operationsTable =
        timeseriesAspectService.getAggregatedStats(
            opContext,
            OPERATIONS_ENTITY_NAME,
            OPERATION_ASPECT_NAME,
            aggregationSpecs,
            filter,
            groupingBuckets);

    if (!operationsTable.hasRows()) {
      return new OperationsAggregationArray();
    }

    if (operationsTable.getColumnNames().size() != 3) {
      throw new RuntimeException(
          "Malformed data returned when getting aggregated operations stats");
    }

    // 4. Create map of timestamp ->  OperationsAggregationsResult so we can aggregate all the
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

  private static OperationsAggregationsResult aggregateByType(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      Filter filter,
      String resource) {
    OperationsAggregationsResult aggregations = new OperationsAggregationsResult();
    AggregationSpec timestampSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("timestampMillis");
    AggregationSpec[] aggregationSpecs = new AggregationSpec[] {timestampSpec};

    GroupingBucket operationTypeBucket =
        new GroupingBucket()
            .setKey("operationType")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);
    GroupingBucket[] groupingBuckets = new GroupingBucket[] {operationTypeBucket};

    // Query backend
    GenericTable operationsTable =
        timeseriesAspectService.getAggregatedStats(
            opContext,
            OPERATIONS_ENTITY_NAME,
            OPERATION_ASPECT_NAME,
            aggregationSpecs,
            filter,
            groupingBuckets);

    if (operationsTable.getRows() == null || operationsTable.getRows().size() == 0) {
      return aggregations;
    }

    if (operationsTable.getColumnNames().size() != 2) {
      throw new RuntimeException(
          "Malformed data returned when getting aggregated operations stats");
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

    aggregations.setTotalOperations(totalOperations);

    return aggregations;
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
      default:
        log.debug(String.format("Found unexpected operation type %s", operationType));
    }
  }
}
