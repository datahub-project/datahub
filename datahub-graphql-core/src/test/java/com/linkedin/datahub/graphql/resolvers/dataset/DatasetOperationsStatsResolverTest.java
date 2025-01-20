package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
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
  private static final OperationsStatsInput TEST_INPUT =
      new OperationsStatsInput(TimeRange.MONTH, null);

  static {
    TEST_SOURCE.setUrn(TEST_DATASET_URN);
  }

  @Test
  public void testGetSuccess() throws Exception {
    TimeseriesAspectService mockService = initMockTimeseriesService();

    // Execute resolver
    DatasetOperationsStatsResolver resolver = new DatasetOperationsStatsResolver(mockService);
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    OperationsQueryResult result = resolver.get(mockEnv).get();

    // Validate aggregations
    Assert.assertEquals(result.getAggregations().getTotalOperations(), 90);
    Assert.assertEquals(result.getAggregations().getTotalInserts(), 37);
    Assert.assertEquals(result.getAggregations().getTotalUpdates(), 9);
    Assert.assertEquals(result.getAggregations().getTotalDeletes(), 12);
    Assert.assertEquals(result.getAggregations().getTotalCustoms(), 10);
    Assert.assertEquals(result.getAggregations().getTotalAlters(), 20);
    Assert.assertEquals(result.getAggregations().getTotalCreates(), 1);
    Assert.assertEquals(result.getAggregations().getTotalDrops(), 1);
    Assert.assertEquals(result.getAggregations().getCustomOperationsMap().size(), 2);
    Assert.assertEquals(
        result.getAggregations().getCustomOperationsMap().get(0).getKey(), "customDelete");
    Assert.assertEquals(result.getAggregations().getCustomOperationsMap().get(0).getValue(), 5);
    Assert.assertEquals(
        result.getAggregations().getCustomOperationsMap().get(1).getKey(), "customInsert");
    Assert.assertEquals(result.getAggregations().getCustomOperationsMap().get(1).getValue(), 5);
    // Validate buckets
    Assert.assertEquals(result.getBuckets().size(), 30);
    // validate first bucket
    Assert.assertEquals(result.getBuckets().get(0).getBucket(), 1731801600000L);
    Assert.assertEquals(result.getBuckets().get(0).getAggregations().getTotalOperations(), 2);
    Assert.assertEquals(result.getBuckets().get(0).getAggregations().getTotalDeletes(), 1);
    Assert.assertEquals(result.getBuckets().get(0).getAggregations().getTotalCustoms(), 1);
    Assert.assertEquals(
        result.getBuckets().get(0).getAggregations().getCustomOperationsMap().size(), 1);
    Assert.assertEquals(
        result.getBuckets().get(0).getAggregations().getCustomOperationsMap().get(0).getKey(),
        "customInsert");
    Assert.assertEquals(
        result.getBuckets().get(0).getAggregations().getCustomOperationsMap().get(0).getValue(), 1);
    // validate last bucket
    Assert.assertEquals(result.getBuckets().get(29).getBucket(), 1734307200000L);
    Assert.assertEquals(result.getBuckets().get(29).getAggregations().getTotalOperations(), 18);
    Assert.assertEquals(result.getBuckets().get(29).getAggregations().getTotalDeletes(), 1);
    Assert.assertEquals(result.getBuckets().get(29).getAggregations().getTotalInserts(), 17);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
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

    // When the user doesn't have permission, return empty results instead of throwing on the whole
    // dataset
    Assert.assertNull(result.getAggregations());
    Assert.assertNull(result.getBuckets());
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
    QueryContext mockContext = getMockAllowContext();

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
                        new StringArray(ImmutableList.of("ALTER", "20")),
                        new StringArray(ImmutableList.of("CREATE", "1")),
                        new StringArray(ImmutableList.of("DROP", "1")),
                        new StringArray(ImmutableList.of("CUSTOM", "10")),
                        new StringArray(ImmutableList.of("UPDATE", "9"))))
                .setColumnNames(
                    new StringArray(
                        ImmutableList.of("operationType", "cardinality_lastUpdatedTimestamp")))
                .setColumnTypes(new StringArray()));
    // mock aggregating overall by customOperationType
    when(mockService.getAggregatedStats(
            any(),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.OPERATION_ASPECT_NAME),
            Mockito.eq(getTimestampAggSpec()),
            any(Filter.class),
            Mockito.eq(getCustomOperationsBuckets())))
        .thenReturn(
            new GenericTable()
                .setRows(
                    new StringArrayArray(
                        new StringArray(ImmutableList.of("customInsert", "5")),
                        new StringArray(ImmutableList.of("customDelete", "5"))))
                .setColumnNames(
                    new StringArray(
                        ImmutableList.of(
                            "customOperationType", "cardinality_lastUpdatedTimestamp")))
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
                        new StringArray(ImmutableList.of("1731801600000", "CUSTOM", "1")),
                        new StringArray(ImmutableList.of("1731888000000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1731974400000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1732060800000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1732147200000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1732233600000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1732233600000", "ALTER", "20")),
                        new StringArray(ImmutableList.of("1732320000000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1732320000000", "CUSTOM", "4")),
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
                        new StringArray(ImmutableList.of("1733184000000", "CUSTOM", "5")),
                        new StringArray(ImmutableList.of("1733270400000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733356800000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1733443200000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733529600000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1733616000000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1733702400000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1733702400000", "CREATE", "1")),
                        new StringArray(ImmutableList.of("1733788800000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1733875200000", "INSERT", "1")),
                        new StringArray(ImmutableList.of("1733961600000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1733961600000", "DROP", "1")),
                        new StringArray(ImmutableList.of("1734048000000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1734134400000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1734220800000", "INSERT", "11")),
                        new StringArray(ImmutableList.of("1734220800000", "UPDATE", "1")),
                        new StringArray(ImmutableList.of("1734307200000", "DELETE", "1")),
                        new StringArray(ImmutableList.of("1734307200000", "INSERT", "17"))))
                .setColumnNames(
                    new StringArray(
                        ImmutableList.of(
                            "lastUpdatedTimestamp",
                            "operationType",
                            "cardinality_lastUpdatedTimestamp")))
                .setColumnTypes(new StringArray()));

    // mock aggregating custom operations by day
    when(mockService.getAggregatedStats(
            any(),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.OPERATION_ASPECT_NAME),
            Mockito.eq(getTimestampAggSpec()),
            any(Filter.class),
            Mockito.eq(getCustomOperationsGroupingBuckets())))
        .thenReturn(
            new GenericTable()
                .setRows(
                    new StringArrayArray(
                        new StringArray(ImmutableList.of("1731801600000", "customInsert", "1")),
                        new StringArray(ImmutableList.of("1732320000000", "customInsert", "4")),
                        new StringArray(ImmutableList.of("1733184000000", "customDelete", "5"))))
                .setColumnNames(
                    new StringArray(
                        ImmutableList.of(
                            "lastUpdatedTimestamp",
                            "customOperationType",
                            "cardinality_lastUpdatedTimestamp")))
                .setColumnTypes(new StringArray()));

    return mockService;
  }

  private AggregationSpec[] getTimestampAggSpec() {
    AggregationSpec timestampSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("lastUpdatedTimestamp");
    return new AggregationSpec[] {timestampSpec};
  }

  private GroupingBucket[] getGroupingBucketsByDay() {
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey("lastUpdatedTimestamp")
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));
    GroupingBucket operationTypeBucket =
        new GroupingBucket()
            .setKey("operationType")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    return new GroupingBucket[] {timestampBucket, operationTypeBucket};
  }

  private GroupingBucket[] getCustomOperationsGroupingBuckets() {
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey("lastUpdatedTimestamp")
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));
    GroupingBucket operationTypeBucket =
        new GroupingBucket()
            .setKey("customOperationType")
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

  private GroupingBucket[] getCustomOperationsBuckets() {
    GroupingBucket operationTypeBucket =
        new GroupingBucket()
            .setKey("customOperationType")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    return new GroupingBucket[] {operationTypeBucket};
  }
}
