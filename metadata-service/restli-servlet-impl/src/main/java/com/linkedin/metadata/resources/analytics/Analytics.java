package com.linkedin.metadata.resources.analytics;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.analytics.GetTimeseriesAggregatedStatsResponse;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.resources.restli.RestliUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
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

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import lombok.extern.slf4j.Slf4j;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.linkedin.metadata.authorization.ApiGroup.TIMESERIES;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.utils.CriterionUtils.validateAndConvert;

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
  private TimeseriesAspectService timeseriesAspectService;

    @Inject
    @Named("authorizerChain")
    private Authorizer authorizer;

    @Inject
    @Named("systemOperationContext")
    private OperationContext systemOperationContext;

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
            final Authentication auth = AuthenticationContext.getAuthentication();
            final OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                            ACTION_GET_TIMESERIES_STATS, entityName), authorizer, auth, true);

            if (!AuthUtil.isAPIAuthorizedEntityType(
                    opContext,
                    TIMESERIES, READ,
                    entityName)) {
                throw new RestLiServiceException(
                        HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity " + entityName);
            }

            log.info("Attempting to query timeseries stats");
          GetTimeseriesAggregatedStatsResponse resp = new GetTimeseriesAggregatedStatsResponse();
          resp.setEntityName(entityName);
          resp.setAspectName(aspectName);
          resp.setAggregationSpecs(new AggregationSpecArray(Arrays.asList(aggregationSpecs)));
          final Filter finalFilter = validateAndConvert(filter);
          if (finalFilter != null) {
            resp.setFilter(finalFilter);
          }
          if (groupingBuckets != null) {
            resp.setGroupingBuckets(new GroupingBucketArray(Arrays.asList(groupingBuckets)));
          }

          GenericTable aggregatedStatsTable =
              timeseriesAspectService.getAggregatedStats(opContext,
                  entityName, aspectName, aggregationSpecs, finalFilter, groupingBuckets);
          resp.setTable(aggregatedStatsTable);
          return resp;
        });
  }
}
