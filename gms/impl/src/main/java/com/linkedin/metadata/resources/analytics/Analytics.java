package com.linkedin.metadata.resources.analytics;

import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationSpecArray;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.analytics.GetTimeseriesAggregatedStatsResponse;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketArray;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import com.google.common.collect.ImmutableList;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;


/**
 * Rest.li entry point: /analytics
 */
@Slf4j
@RestLiSimpleResource(name = "analytics", namespace = "com.linkedin.analytics")
public class Analytics extends SimpleResourceTemplate<GetTimeseriesAggregatedStatsResponse> {
  private static final String ACTION_GET_TEMPORAL_STATS = "getTemporalStats";
  private static final String PARAM_ENTITY_NAME= "entityName";
  private static final String PARAM_ASPECT_NAME= "aspectName";
  private static final String PARAM_FILTER = "filter";
  private static final String PARAM_METRICS = "metrics";
  private static final String PARAM_BUCKETS = "buckets";
  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Action(name = ACTION_GET_TEMPORAL_STATS)
  @Nonnull
  public Task<GetTimeseriesAggregatedStatsResponse> getTemporalStats(
      @ActionParam(PARAM_ENTITY_NAME) @Nonnull String entityName,
      @ActionParam(PARAM_ASPECT_NAME) @Nonnull String aspectName,
      @ActionParam(PARAM_METRICS) @Nonnull AggregationSpec[] aggregationSpecs,
      @ActionParam(PARAM_FILTER)  @Optional @Nullable Filter filter,
      @ActionParam(PARAM_BUCKETS)  @Optional @Nullable GroupingBucket[] groupingBuckets) {
    return RestliUtils.toTask(() -> {
      log.info("Attempting to query timeseries stats");
      GetTimeseriesAggregatedStatsResponse resp = new GetTimeseriesAggregatedStatsResponse();
      resp.setEntityName(entityName);
      resp.setAspectName(aspectName);
      resp.setAggregationSpecs(new AggregationSpecArray(Arrays.asList(aggregationSpecs)));
      resp.setFilter(filter);
      resp.setGroupingBuckets(new GroupingBucketArray(Arrays.asList(groupingBuckets)));

      // Sample 1:  Bucketization by latest timestamp for rowCount

      if (Arrays.stream(groupingBuckets).anyMatch(t->t.isStringGroupingBucket() && t.getStringGroupingBucket().getKey().equals("columnStats.key"))) {
        /** Sample Request:
         {
         "entityName" : "table1",
         "aspectName" : "timeseriesStats",
         "metrics" : ["rowCount", "columnStats.numNull"],
         "buckets" : [
         {"com.linkedin.timeseries.DateGroupingBucket": {"key" : "eventTimestampMillis", "granularity": "86400000"}},
         {"com.linkedin.timeseries.StringGroupingBucket": {"key" : "columnStats.key"}}
         ],
         "filter" : { "criteria": [
         {"field": "urn", "value": "table1", "condition": "EQUAL"},
         {"field": "eventTimestampMillis", "value": "1626732371596", "condition": "LESS_THAN"},
         {"field": "eventTimestampMillis", "value": "1625955036000", "condition": "GREATER_THAN_OR_EQUAL_TO"}
         ]}
         }
         */
        GenericTable t1 = new GenericTable();
        t1.setColumnNames(new StringArray(ImmutableList.of("eventTimestampMillils", "rowCount", "columnStats.key", "columnStats.numNull")));
        t1.setColumnTypes(new StringArray(ImmutableList.of("long", "long", "string", "long")));
        t1.setRows(new StringArrayArray(new StringArray(ImmutableList.of("1625875200000", "50", "col1", "10")), new StringArray(ImmutableList.of("1625961600000", "50", "col1", "20")),
            new StringArray(ImmutableList.of("1626048000000", "120", "col1", "30"))));
        resp.setLastUpdatedTimeMillis(1625875200000L);
        resp.setTable(t1);
      }
      else {
        /** Sample Request:
         {
         "entityName" : "table1",
         "aspectName" : "timeseriesStats",
         "metrics" : ["rowCount"],
         "buckets" : [
         {"com.linkedin.timeseries.DateGroupingBucket": {"key" : "eventTimestampMillis", "granularity": "86400000"}}
         ],
         "filter" : { "criteria": [
         {"field": "urn", "value": "table1", "condition": "EQUAL"},
         {"field": "eventTimestampMillis", "value": "1626732371596", "condition": "LESS_THAN"},
         {"field": "eventTimestampMillis", "value": "1625955036000", "condition": "GREATER_THAN_OR_EQUAL_TO"}
         ]}
         }
         */

        /** TODO: Delete this mock.
         GenericTable t2 = new GenericTable();
         t2.setColumnNames(new StringArray(ImmutableList.of("eventTimestampMillis", "rowCount")));
         t2.setColumnTypes(new StringArray(ImmutableList.of("long", "long")));
         t2.setRows(new StringArrayArray(new StringArray(ImmutableList.of("1625875200000", "50")), new StringArray(ImmutableList.of("1625961600000", "50")),
         new StringArray(ImmutableList.of("1626048000000", "120"))));
         resp.setTable(t2);
         */
        GenericTable aggregatedStatsTable = _timeseriesAspectService.getAggregatedStats(entityName, aspectName, aggregationSpecs, filter, groupingBuckets);
        resp.setTable(aggregatedStatsTable);

      }
      return resp;
    });
  }
}