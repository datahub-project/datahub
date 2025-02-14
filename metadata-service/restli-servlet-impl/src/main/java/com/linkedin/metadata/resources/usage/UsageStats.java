package com.linkedin.metadata.resources.usage;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.datahub.authorization.AuthUtil.isAPIAuthorizedEntityUrns;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static com.linkedin.metadata.timeseries.elastic.UsageServiceUtil.USAGE_STATS_ASPECT_NAME;
import static com.linkedin.metadata.timeseries.elastic.UsageServiceUtil.USAGE_STATS_ENTITY_NAME;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.DatasetFieldUsageCounts;
import com.linkedin.dataset.DatasetFieldUsageCountsArray;
import com.linkedin.dataset.DatasetUsageStatistics;
import com.linkedin.dataset.DatasetUserUsageCounts;
import com.linkedin.dataset.DatasetUserUsageCountsArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.resources.restli.RestliUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.UsageServiceUtil;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;
import com.linkedin.timeseries.TimeWindowSize;
import com.linkedin.usage.FieldUsageCounts;
import com.linkedin.usage.UsageAggregation;
import com.linkedin.usage.UsageAggregationMetrics;
import com.linkedin.usage.UsageQueryResult;
import com.linkedin.usage.UsageTimeRange;
import com.linkedin.usage.UserUsageCounts;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

  @Inject
  @Named("entityRegistry")
  private EntityRegistry _entityRegistry;

  @Inject
  @Named("authorizerChain")
  private Authorizer _authorizer;

  @Inject
  @Named("systemOperationContext")
  private OperationContext systemOperationContext;

  @Getter(lazy = true)
  private final AspectSpec usageStatsAspectSpec =
      _entityRegistry.getEntitySpec(USAGE_STATS_ENTITY_NAME).getAspectSpec(USAGE_STATS_ASPECT_NAME);

  @Action(name = ACTION_BATCH_INGEST)
  @Deprecated
  @Nonnull
  @WithSpan
  public Task<Void> batchIngest(@ActionParam(PARAM_BUCKETS) @Nonnull UsageAggregation[] buckets) {
    log.info("Ingesting {} usage stats aggregations", buckets.length);
    return RestliUtils.toTask(systemOperationContext,
        () -> {

          final Authentication auth = AuthenticationContext.getAuthentication();
          String actorUrnStr = auth.getActor().toUrnStr();
          Set<Urn> urns = Arrays.stream(buckets).sequential().map(UsageAggregation::getResource).collect(Collectors.toSet());
          final OperationContext opContext = OperationContext.asSession(
                  systemOperationContext, RequestContext.builder().buildRestli(actorUrnStr, getContext(),
                          ACTION_BATCH_INGEST, urns.stream().map(Urn::getEntityType).collect(Collectors.toList())), _authorizer,
                  auth, true);

          if (!isAPIAuthorizedEntityUrns(
                  opContext,
                  UPDATE,
                  urns)) {
            throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN, "User " + actorUrnStr + " is unauthorized to edit entities.");
          }

          for (UsageAggregation agg : buckets) {
            this.ingest(opContext, agg);
          }
          return null;
        },
        MetricRegistry.name(this.getClass(), "batchIngest"));
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
    log.info(
        "Querying usage stats for resource: {}, duration: {}, start time: {}, end time: {}, max buckets: {}",
        resource, duration, startTime, endTime, maxBuckets);
    return RestliUtils.toTask(systemOperationContext,
        () -> {

          Urn resourceUrn = UrnUtils.getUrn(resource);
          final Authentication auth = AuthenticationContext.getAuthentication();
          final OperationContext opContext = OperationContext.asSession(
                  systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                          ACTION_QUERY, resourceUrn.getEntityType()), _authorizer, auth, true);

          if (!isAPIAuthorized(
                  opContext,
                  PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE,
                  new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString()))) {
            throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN, "User is unauthorized to query usage.");
          }

          return UsageServiceUtil.query(opContext, _timeseriesAspectService, resource, duration, startTime, endTime, maxBuckets);
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

    Urn resourceUrn = UrnUtils.getUrn(resource);
    final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    ACTION_QUERY_RANGE, resourceUrn.getEntityType()), _authorizer, auth, true);


    if (!isAPIAuthorized(
            opContext,
            PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE,
            new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString()))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to query usage.");
    }

    return RestliUtils.toTask(systemOperationContext,
            () -> UsageServiceUtil.queryRange(opContext, _timeseriesAspectService, resource, duration, range), MetricRegistry.name(this.getClass(), "queryRange"));
  }

  private void ingest(@Nonnull OperationContext opContext, @Nonnull UsageAggregation bucket) {
    // 1. Translate the bucket to DatasetUsageStatistics first.
    DatasetUsageStatistics datasetUsageStatistics = new DatasetUsageStatistics();
    datasetUsageStatistics.setTimestampMillis(bucket.getBucket());
    datasetUsageStatistics.setEventGranularity(
        new TimeWindowSize().setUnit(UsageServiceUtil.windowToInterval(bucket.getDuration())).setMultiple(1));
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
              bucket.getResource(), datasetUsageStatistics, getUsageStatsAspectSpec(), null,
                  systemOperationContext.getSearchContext().getIndexConvention().getIdHashAlgo());
    } catch (JsonProcessingException e) {
      log.error("Failed to generate timeseries document from aspect: {}", e.toString());
      return;
    }
    // 3. Upsert the exploded documents to timeseries aspect service.
    documents
        .entrySet()
        .forEach(
            document -> {
              _timeseriesAspectService.upsertDocument(opContext,
                  USAGE_STATS_ENTITY_NAME,
                  USAGE_STATS_ASPECT_NAME,
                  document.getKey(),
                  document.getValue());
            });
  }
}
