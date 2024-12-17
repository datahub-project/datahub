package com.linkedin.datahub.graphql.resolvers.dataset;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.OperationsQueryResult;
import com.linkedin.datahub.graphql.generated.OperationsStatsInput;
import com.linkedin.datahub.graphql.generated.TimeRange;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatasetOperationsStatsResolverTest {

  private static final Dataset TEST_SOURCE = new Dataset();
  private static final String TEST_DATASET_URN = "urn:li:dataset:(test,test,test)";
  private static final OperationsStatsInput TEST_INPUT = new OperationsStatsInput(TimeRange.MONTH);

  static {
    TEST_SOURCE.setUrn(TEST_DATASET_URN);
  }

  @Test
  public void testGetSuccess() throws Exception {
    TimeseriesAspectService mockService = initMockTimeseriesService();

    // Execute resolver
    DatasetOperationsStatsResolver resolver = new DatasetOperationsStatsResolver(mockService);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    OperationsQueryResult result = resolver.get(mockEnv).get();

    // Validate aggregations
    Assert.assertEquals(result.getAggregations().getTotalOperations(), 58);
    Assert.assertEquals(result.getAggregations().getTotalInserts(), 37);
    Assert.assertEquals(result.getAggregations().getTotalUpdates(), 9);
    Assert.assertEquals(result.getAggregations().getTotalDeletes(), 12);
    // Validate buckets
    Assert.assertEquals(result.getBuckets().size(), 30);
    // validate first bucket
    Assert.assertEquals(result.getBuckets().get(0).getBucket(), 1731801600000L);
    Assert.assertEquals(result.getBuckets().get(0).getAggregations().getTotalOperations(), 1);
    Assert.assertEquals(result.getBuckets().get(0).getAggregations().getTotalDeletes(), 1);
    // validate last bucket
    Assert.assertEquals(result.getBuckets().get(29).getBucket(), 1734307200000L);
    Assert.assertEquals(result.getBuckets().get(29).getAggregations().getTotalOperations(), 18);
    Assert.assertEquals(result.getBuckets().get(29).getAggregations().getTotalDeletes(), 1);
    Assert.assertEquals(result.getBuckets().get(29).getAggregations().getTotalInserts(), 17);
  }

  @Test
  public void testGetException() throws Exception {
    TimeseriesAspectService mockService = mock(TimeseriesAspectService.class);
    when(mockService.getAggregatedStats(
            any(),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.OPERATION_ASPECT_NAME),
            Mockito.eq(getTimestampAggSpec()),
            any(Filter.class),
            Mockito.eq(getGroupingBucketsByType())))
        .thenThrow(new RuntimeException("Error!"));

    // Execute resolver
    DatasetOperationsStatsResolver resolver = new DatasetOperationsStatsResolver(mockService);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    OperationsQueryResult result = resolver.get(mockEnv).get();

    // When timeseries service throws, catch in the resolver and return an empty query result
    Assert.assertNull(result.getAggregations());
    Assert.assertNull(result.getBuckets());
  }

  private TimeseriesAspectService initMockTimeseriesService() {
    TimeseriesAspectService mockService = mock(TimeseriesAspectService.class);

    // mock aggregating overall by type
    when(mockService.getAggregatedStats(
            any(),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.OPERATION_ASPECT_NAME),
            Mockito.eq(getTimestampAggSpec()),
            any(Filter.class),
            Mockito.eq(getGroupingBucketsByType())))
        .thenReturn(
            new GenericTable()
                .setRows(
                    new StringArrayArray(
                        new StringArray(ImmutableList.of("DELETE", "12")),
                        new StringArray(ImmutableList.of("INSERT", "37")),
                        new StringArray(ImmutableList.of("UPDATE", "9"))))
                .setColumnNames(
                    new StringArray(
                        ImmutableList.of("operationType", "cardinality_timestampMillis")))
                .setColumnTypes(new StringArray()));

    // mock aggregating by day
    when(mockService.getAggregatedStats(
            any(),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.OPERATION_ASPECT_NAME),
            Mockito.eq(getTimestampAggSpec()),
            any(Filter.class),
            Mockito.eq(getGroupingBucketsByDay())))
        .thenReturn(
            new GenericTable()
                .setRows(
                    new StringArrayArray(
                        new StringArray(ImmutableList.of("1731801600000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1731888000000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1731974400000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1732060800000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1732147200000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1732233600000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1732320000000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1732406400000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1732492800000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1732579200000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1732665600000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1732752000000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1732838400000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1732924800000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733011200000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1733097600000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733184000000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733270400000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733356800000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1733443200000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733529600000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1733616000000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1733702400000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1733788800000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733875200000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1733961600000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1734048000000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1734134400000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1734220800000", "INSERT", "11")),
                        new StringArray(ImmutableList.of("1734220800000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1734307200000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1734307200000", "INSERT", "17"))))
                .setColumnNames(
                    new StringArray(
                        ImmutableList.of(
                            "timestampMillis", "operationType", "cardinality_timestampMillis")))
                .setColumnTypes(new StringArray()));

    return mockService;
  }

  private AggregationSpec[] getTimestampAggSpec() {
    AggregationSpec timestampSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath(com.linkedin.metadata.timeseries.elastic.Constants.ES_FIELD_TIMESTAMP);
    return new AggregationSpec[] {timestampSpec};
  }

  private GroupingBucket[] getGroupingBucketsByDay() {
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(com.linkedin.metadata.timeseries.elastic.Constants.ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));
    GroupingBucket operationTypeBucket =
        new GroupingBucket()
            .setKey("operationType")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    return new GroupingBucket[] {timestampBucket, operationTypeBucket};
  }

  private GroupingBucket[] getGroupingBucketsByType() {
    GroupingBucket operationTypeBucket =
        new GroupingBucket()
            .setKey("operationType")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    return new GroupingBucket[] {operationTypeBucket};
  }
}
