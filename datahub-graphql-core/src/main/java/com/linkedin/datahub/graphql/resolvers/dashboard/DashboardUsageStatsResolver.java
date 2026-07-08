package com.linkedin.datahub.graphql.resolvers.dashboard;

import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregation;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.generated.DashboardUsageQueryResult;
import com.linkedin.datahub.graphql.generated.DashboardUsageQueryResultAggregations;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.resolvers.load.DashboardUsageBucketsBatchLoader;
import com.linkedin.datahub.graphql.resolvers.load.TimeseriesAspectBatchLoader;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.DataLoader;

/**
 * Resolver used for resolving the usage statistics of a Dashboard.
 *
 * <p>Returns daily as well as absolute usage metrics of Dashboard
 */
@Slf4j
public class DashboardUsageStatsResolver
    implements DataFetcher<CompletableFuture<DashboardUsageQueryResult>> {
  private final TimeseriesAspectService timeseriesAspectService;

  // When true, the absolute-usage-metrics fetch is routed through the shared
  // TimeseriesAspectBatchLoader so a page of dashboards collapses to one top_hits aggregation
  // instead of one getAspectValues per dashboard. Gated by
  // featureFlags.timeseriesAspectBatchLoadEnabled
  // so it reverts to the per-URN path with the same switch as TimeSeriesAspectResolver.
  private final boolean batchLoadEnabled;

  // When true, the daily-usage time buckets are routed through DashboardUsageBucketsBatchLoader so
  // a
  // page of dashboards collapses to one batchGetAggregatedStats per time window instead of one
  // getAggregatedStats per dashboard. Gated by featureFlags.timeseriesAspectAggBatchLoadEnabled
  // (the
  // same switch DashboardStatsSummaryResolver uses). The per-user aggregation stays per-URN.
  private final boolean aggBatchLoadEnabled;

  public DashboardUsageStatsResolver(
      final TimeseriesAspectService timeseriesAspectService,
      final boolean batchLoadEnabled,
      final boolean aggBatchLoadEnabled) {
    this.timeseriesAspectService = timeseriesAspectService;
    this.batchLoadEnabled = batchLoadEnabled;
    this.aggBatchLoadEnabled = aggBatchLoadEnabled;
  }

  @Override
  public CompletableFuture<DashboardUsageQueryResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String dashboardUrn = ((Entity) environment.getSource()).getUrn();
    final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
    final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
    // Max number of aspects to return for absolute dashboard usage.
    final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);

    // Enqueue the absolute-usage-metrics timeseries load eagerly, during field fetching, so the
    // DataLoader key is queued before graphql-java's dispatch tick. A key queued later (inside the
    // supplyAsync continuation below) would never be dispatched and the request would hang. Null
    // when batching is disabled or the loader is unregistered; the per-URN fallback handles that.
    final CompletableFuture<List<EnvelopedAspect>> metricsFuture =
        maybeLoadUsageMetrics(
            environment, dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis, maybeLimit);

    // Enqueue the daily-buckets aggregation eagerly too, for the same dispatch-timing reason as the
    // metrics load. Null when agg-batching is disabled or the loader is unregistered.
    final CompletableFuture<List<DashboardUsageAggregation>> bucketsFuture =
        maybeLoadBuckets(environment, dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis);

    // Time-bucket stats and per-user aggregations run concurrently with the metrics load. Only the
    // buckets query is batchable; the per-user aggregation stays per-URN (see getAggregations).
    final CompletableFuture<DashboardUsageQueryResult> bucketStatsFuture =
        resolveBucketStats(
            context, dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis, bucketsFuture);

    final CompletableFuture<List<DashboardUsageMetrics>> metricsResultFuture =
        resolveUsageMetrics(
            context,
            dashboardUrn,
            maybeStartTimeMillis,
            maybeEndTimeMillis,
            maybeLimit,
            metricsFuture);

    return bucketStatsFuture.thenCombine(
        metricsResultFuture,
        (usageQueryResult, dashboardUsageMetrics) -> {
          usageQueryResult.setMetrics(dashboardUsageMetrics);
          return usageQueryResult;
        });
  }

  /**
   * Enqueues the absolute usage-metrics doc fetch on the shared {@link TimeseriesAspectBatchLoader}
   * so a page of dashboards collapses to one top_hits aggregation instead of one getAspectValues
   * per dashboard. Returns null when batching is disabled or the loader is not registered, in which
   * case {@link #resolveUsageMetrics} uses the per-URN path.
   *
   * <p>Must be called during field fetching (from {@code get}) so the key is queued before
   * graphql-java's dispatch tick. The loader scopes by URN via its own terms aggregation, so the
   * shared filter must be URN-free (see {@link DashboardUsageStatsUtils#buildSharedUsageFilter}).
   */
  @Nullable
  private CompletableFuture<List<EnvelopedAspect>> maybeLoadUsageMetrics(
      final DataFetchingEnvironment environment,
      final String dashboardUrn,
      @Nullable final Long maybeStartTimeMillis,
      @Nullable final Long maybeEndTimeMillis,
      @Nullable final Integer maybeLimit) {
    if (!batchLoadEnabled) {
      return null;
    }
    final DataLoader<TimeseriesAspectBatchLoader.Key, List<EnvelopedAspect>> loader =
        environment.getDataLoaderRegistry().getDataLoader(TimeseriesAspectBatchLoader.LOADER_NAME);
    if (loader == null) {
      return null;
    }
    final TimeseriesAspectBatchLoader.Key key =
        new TimeseriesAspectBatchLoader.Key(
            dashboardUrn,
            Constants.DASHBOARD_ENTITY_NAME,
            Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
            maybeStartTimeMillis,
            maybeEndTimeMillis,
            maybeLimit,
            buildSharedUsageFilter(null, null, false),
            null);
    return loader.load(key);
  }

  /**
   * Enqueues the daily-buckets aggregation on the {@link DashboardUsageBucketsBatchLoader} so a
   * page of dashboards collapses to one {@code batchGetAggregatedStats} per time window. Returns
   * null when agg-batching is disabled or the loader is not registered, in which case {@link
   * #resolveBucketStats} uses the per-URN {@code getBuckets} path.
   *
   * <p>Must be called during field fetching (from {@code get}) so the key is queued before
   * graphql-java's dispatch tick.
   */
  @Nullable
  private CompletableFuture<List<DashboardUsageAggregation>> maybeLoadBuckets(
      final DataFetchingEnvironment environment,
      final String dashboardUrn,
      @Nullable final Long maybeStartTimeMillis,
      @Nullable final Long maybeEndTimeMillis) {
    if (!aggBatchLoadEnabled) {
      return null;
    }
    final DataLoader<DashboardUsageBucketsBatchLoader.Key, List<DashboardUsageAggregation>> loader =
        environment
            .getDataLoaderRegistry()
            .getDataLoader(DashboardUsageBucketsBatchLoader.LOADER_NAME);
    if (loader == null) {
      return null;
    }
    return loader.load(
        new DashboardUsageBucketsBatchLoader.Key(
            dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis));
  }

  /**
   * Builds the buckets + aggregations half of the result. When {@code bucketsFuture} is present the
   * daily buckets come from the batch loader; otherwise they are fetched per-URN. The per-user
   * aggregation inside {@link DashboardUsageStatsUtils#getAggregations} stays per-URN either way,
   * so it must run on the async executor rather than the DataLoader completion thread.
   */
  private CompletableFuture<DashboardUsageQueryResult> resolveBucketStats(
      final QueryContext context,
      final String dashboardUrn,
      @Nullable final Long maybeStartTimeMillis,
      @Nullable final Long maybeEndTimeMillis,
      @Nullable final CompletableFuture<List<DashboardUsageAggregation>> bucketsFuture) {
    final Filter bucketStatsFilter =
        createUsageFilter(dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis, true);
    if (bucketsFuture != null) {
      return bucketsFuture.thenCompose(
          dailyUsageBuckets ->
              GraphQLConcurrencyUtils.supplyAsync(
                  () -> buildBucketStats(context, bucketStatsFilter, dailyUsageBuckets),
                  this.getClass().getSimpleName(),
                  "resolveBucketStats"));
    }
    // Per-URN fallback: agg-batching disabled or loader unregistered.
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final List<DashboardUsageAggregation> dailyUsageBuckets =
              getBuckets(
                  context.getOperationContext(),
                  bucketStatsFilter,
                  dashboardUrn,
                  timeseriesAspectService);
          return buildBucketStats(context, bucketStatsFilter, dailyUsageBuckets);
        },
        this.getClass().getSimpleName(),
        "resolveBucketStats");
  }

  private DashboardUsageQueryResult buildBucketStats(
      final QueryContext context,
      final Filter bucketStatsFilter,
      final List<DashboardUsageAggregation> dailyUsageBuckets) {
    final DashboardUsageQueryResult usageQueryResult = new DashboardUsageQueryResult();
    final DashboardUsageQueryResultAggregations aggregations =
        getAggregations(
            context.getOperationContext(),
            bucketStatsFilter,
            dailyUsageBuckets,
            timeseriesAspectService);
    usageQueryResult.setBuckets(dailyUsageBuckets);
    usageQueryResult.setAggregations(aggregations);
    return usageQueryResult;
  }

  private CompletableFuture<List<DashboardUsageMetrics>> resolveUsageMetrics(
      final QueryContext context,
      final String dashboardUrn,
      @Nullable final Long maybeStartTimeMillis,
      @Nullable final Long maybeEndTimeMillis,
      @Nullable final Integer maybeLimit,
      @Nullable final CompletableFuture<List<EnvelopedAspect>> metricsFuture) {
    if (metricsFuture != null) {
      return metricsFuture.thenApply(aspects -> mapDashboardUsageMetrics(context, aspects));
    }
    // Per-URN fallback: batching disabled or loader unregistered.
    return GraphQLConcurrencyUtils.supplyAsync(
        () ->
            getDashboardUsageMetrics(
                context,
                dashboardUrn,
                maybeStartTimeMillis,
                maybeEndTimeMillis,
                maybeLimit,
                timeseriesAspectService),
        this.getClass().getSimpleName(),
        "resolveUsageMetrics");
  }
}
