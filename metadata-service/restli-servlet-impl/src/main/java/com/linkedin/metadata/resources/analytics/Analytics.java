package com.linkedin.metadata.resources.analytics;

import com.linkedin.analytics.GetTimeseriesAggregatedStatsResponse;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.resources.restli.RestliUtils;
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
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketArray;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

/** Rest.li entry point: /analytics */
@Slf4j
@RestLiSimpleResource(name = "analytics", namespace = "com.linkedin.analytics")
public class Analytics extends SimpleResourceTemplate<GetTimeseriesAggregatedStatsResponse> {
  private static final String ACTION_GET_TIMESERIES_STATS = "getTimeseriesStats";
  private static final String PARAM_ENTITY_NAME = "entityName";
  private static final String PARAM_ASPECT_NAME = "aspectName";
  private static final String PARAM_FILTER = "filter";
  private static final String PARAM_METRICS = "metrics";
  private static final String PARAM_BUCKETS = "buckets";

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Action(name = ACTION_GET_TIMESERIES_STATS)
  @Nonnull
  public Task<GetTimeseriesAggregatedStatsResponse> getTimeseriesStats(
      @ActionParam(PARAM_ENTITY_NAME) @Nonnull String entityName,
      @ActionParam(PARAM_ASPECT_NAME) @Nonnull String aspectName,
      @ActionParam(PARAM_METRICS) @Nonnull AggregationSpec[] aggregationSpecs,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_BUCKETS) @Optional @Nullable GroupingBucket[] groupingBuckets) {
    return RestliUtils.toTask(
        () -> {
          log.info("Attempting to query timeseries stats");
          GetTimeseriesAggregatedStatsResponse resp = new GetTimeseriesAggregatedStatsResponse();
          resp.setEntityName(entityName);
          resp.setAspectName(aspectName);
          resp.setAggregationSpecs(new AggregationSpecArray(Arrays.asList(aggregationSpecs)));
          if (filter != null) {
            resp.setFilter(filter);
          }
          if (groupingBuckets != null) {
            resp.setGroupingBuckets(new GroupingBucketArray(Arrays.asList(groupingBuckets)));
          }

          GenericTable aggregatedStatsTable =
              _timeseriesAspectService.getAggregatedStats(
                  entityName, aspectName, aggregationSpecs, filter, groupingBuckets);
          resp.setTable(aggregatedStatsTable);
          return resp;
        });
  }
}
