package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DashboardStatsSummary;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.types.dashboard.mappers.DashboardUsageMetricMapper;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.GenericTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * DataLoader that batches {@code Dashboard.statsSummary} fetches across all dashboards in a single
 * GraphQL request. Instead of N separate ES calls per dashboard, it fires three batch calls:
 *
 * <ol>
 *   <li>{@code batchGetAspectValues} (limit=1) → {@code viewCount}
 *   <li>{@code batchGetAggregatedStats} with CARDINALITY on {@code userCounts.user} → {@code
 *       uniqueUserCountLast30Days}
 *   <li>{@code batchGetAggregatedStats} with STRING grouping (size=5, ES-ordered by metric DESC) on
 *       {@code userCounts.user} → {@code topUsersLast30Days}
 * </ol>
 */
@Slf4j
@RequiredArgsConstructor
public class DashboardStatsSummaryBatchLoader {

  public static final String LOADER_NAME = "DashboardStatsSummary";

  private static final int MAX_TOP_USERS = 5;

  private final TimeseriesAspectService timeseriesAspectService;

  public static DataLoader<Urn, DashboardStatsSummary> createDataLoader(
      final TimeseriesAspectService timeseriesAspectService, final QueryContext queryContext) {
    final DashboardStatsSummaryBatchLoader loader =
        new DashboardStatsSummaryBatchLoader(timeseriesAspectService);
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> loader.batchLoad(keys, (QueryContext) env.getContext()),
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  public List<DashboardStatsSummary> batchLoad(final List<Urn> urns, final QueryContext context) {
    final long now = System.currentTimeMillis();
    final long start = timeMinusOneMonth(now);

    final Filter viewCountFilter = buildSharedUsageFilter(null, null, false);
    final Filter statsFilter = buildSharedUsageFilter(start, now, true);

    // Fire all three ES queries concurrently — they are independent of each other
    // Call A: latest aspect document per URN for viewCount (absolute stats, no time bucket)
    final CompletableFuture<Map<Urn, List<EnvelopedAspect>>> aspectFuture =
        GraphQLConcurrencyUtils.supplyAsync(
            () ->
                timeseriesAspectService.batchGetAspectValues(
                    context.getOperationContext(),
                    new HashSet<>(urns),
                    Constants.DASHBOARD_ENTITY_NAME,
                    Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
                    null,
                    null,
                    1,
                    viewCountFilter,
                    null),
            LOADER_NAME,
            "batchGetAspectValues");

    // Call B: cardinality of userCounts.user → uniqueUserCountLast30Days
    final CompletableFuture<Map<Urn, GenericTable>> cardinalityFuture =
        GraphQLConcurrencyUtils.supplyAsync(
            () ->
                timeseriesAspectService.batchGetAggregatedStats(
                    context.getOperationContext(),
                    Constants.DASHBOARD_ENTITY_NAME,
                    Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
                    getUniqueUserCountAggSpecs(),
                    urns,
                    statsFilter,
                    null),
            LOADER_NAME,
            "batchGetAggregatedStats:cardinality");

    // Call C: STRING grouping (size=MAX_TOP_USERS, ES-ordered by metric DESC) → topUsersLast30Days
    final CompletableFuture<Map<Urn, GenericTable>> topUsersFuture =
        GraphQLConcurrencyUtils.supplyAsync(
            () ->
                timeseriesAspectService.batchGetAggregatedStats(
                    context.getOperationContext(),
                    Constants.DASHBOARD_ENTITY_NAME,
                    Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
                    getTopUsersSumAggSpecs(),
                    urns,
                    statsFilter,
                    getTopUsersGroupingBuckets(MAX_TOP_USERS)),
            LOADER_NAME,
            "batchGetAggregatedStats:topUsers");

    CompletableFuture.allOf(aspectFuture, cardinalityFuture, topUsersFuture).join();

    final Map<Urn, List<EnvelopedAspect>> aspectResults = aspectFuture.join();
    final Map<Urn, GenericTable> cardinalityResults = cardinalityFuture.join();
    final Map<Urn, GenericTable> topUsersResults = topUsersFuture.join();

    final List<DashboardStatsSummary> results = new ArrayList<>(urns.size());
    for (Urn urn : urns) {
      results.add(buildSummary(urn, aspectResults, cardinalityResults, topUsersResults));
    }
    return results;
  }

  private DashboardStatsSummary buildSummary(
      final Urn urn,
      final Map<Urn, List<EnvelopedAspect>> aspectResults,
      final Map<Urn, GenericTable> cardinalityResults,
      final Map<Urn, GenericTable> topUsersResults) {

    final DashboardStatsSummary summary = new DashboardStatsSummary();

    // viewCount: latest aspect document's viewsCount
    final List<EnvelopedAspect> aspects = aspectResults.getOrDefault(urn, Collections.emptyList());
    if (!aspects.isEmpty()) {
      final DashboardUsageMetrics metrics = DashboardUsageMetricMapper.map(null, aspects.get(0));
      if (metrics.getViewsCount() != null) {
        summary.setViewCount(metrics.getViewsCount());
      }
    }

    // uniqueUserCountLast30Days: cardinality value (single row, single column)
    final GenericTable cardTable = cardinalityResults.get(urn);
    if (cardTable != null && !cardTable.getRows().isEmpty()) {
      final var row = cardTable.getRows().get(0);
      if (!row.isEmpty() && !ES_NULL_VALUE.equals(row.get(0))) {
        try {
          summary.setUniqueUserCountLast30Days((int) Long.parseLong(row.get(0)));
        } catch (NumberFormatException e) {
          log.warn("Failed to parse uniqueUserCount for {}: {}", urn, row.get(0));
        }
      }
    }

    // topUsersLast30Days: ES returns exactly MAX_TOP_USERS rows already ordered by usage DESC
    final GenericTable topTable = topUsersResults.get(urn);
    if (topTable != null && !topTable.getRows().isEmpty()) {
      summary.setTopUsersLast30Days(buildTopUsersFromBatchAggResult(topTable));
    }

    return summary;
  }
}
