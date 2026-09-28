package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregation;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.GenericTable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * DataLoader that batches the daily-usage time-bucket fetch for {@code Dashboard.usageStats} across
 * all dashboards in a single GraphQL request. Replaces the per-URN {@code getAggregatedStats} call
 * in {@link com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils#getBuckets}
 * with one {@code batchGetAggregatedStats} (outer {@code terms(urn)} + inner daily date-histogram)
 * per distinct time window.
 *
 * <p>Only the daily-buckets aggregation is batched here. The per-user aggregation ({@code
 * getUserUsageCounts}) stays per-URN because it returns the full, uncapped user list and would blow
 * the {@code urns × user_cardinality} bucket budget under a batched terms aggregation.
 *
 * <p>Keys carry the time window because {@code batchGetAggregatedStats} applies one shared filter
 * to the whole batch; dashboards queried with different {@code startTimeMillis}/{@code
 * endTimeMillis} must land in separate batch groups so each gets the correct window.
 */
@Slf4j
@RequiredArgsConstructor
public class DashboardUsageBucketsBatchLoader {

  public static final String LOADER_NAME = "DashboardUsageBuckets";

  private final TimeseriesAspectService timeseriesAspectService;

  /** Per-dashboard key; the time window participates in equality so windows group separately. */
  @Value
  public static class Key {
    String urn;
    @Nullable Long startTimeMillis;
    @Nullable Long endTimeMillis;
  }

  public static DataLoader<Key, List<DashboardUsageAggregation>> createDataLoader(
      final TimeseriesAspectService timeseriesAspectService, final QueryContext queryContext) {
    final DashboardUsageBucketsBatchLoader loader =
        new DashboardUsageBucketsBatchLoader(timeseriesAspectService);
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

  public List<List<DashboardUsageAggregation>> batchLoad(
      final List<Key> keys, final QueryContext context) {
    // Group keys by time window: batchGetAggregatedStats takes one shared filter per call, so each
    // distinct (start, end) is a separate batch call over the URNs that share that window.
    final Map<TimeWindow, List<Key>> byWindow = new LinkedHashMap<>();
    for (Key key : keys) {
      byWindow
          .computeIfAbsent(
              new TimeWindow(key.getStartTimeMillis(), key.getEndTimeMillis()),
              w -> new ArrayList<>())
          .add(key);
    }

    final Map<Key, List<DashboardUsageAggregation>> resultsByKey = new java.util.HashMap<>();
    for (Map.Entry<TimeWindow, List<Key>> entry : byWindow.entrySet()) {
      final TimeWindow window = entry.getKey();
      final List<Urn> urns =
          entry.getValue().stream()
              .map(k -> UrnUtils.getUrn(k.getUrn()))
              .distinct()
              .collect(Collectors.toList());

      if (urns.size() == 1) {
        // A single URN has no fan-out to collapse: the outer terms(batch_urn_outer) wrap and the
        // DataLoader dispatch would be pure overhead (measured ~+50% p50 on the single-dashboard
        // detail view, which is the only caller pattern this field sees today). Use the per-URN
        // path — identical to the flag-off fallback. This mirrors TimeseriesAspectBatchLoader's
        // `indices.size() == 1` guard. A higher floor is environment-dependent (per-query
        // round-trip
        // cost, data volume) and would need a config knob measured in the target cluster, not a
        // constant derived from a local benchmark, so 1 is the only portable threshold.
        final String urn = entry.getValue().get(0).getUrn();
        final Filter urnFilter =
            createUsageFilter(urn, window.getStartTimeMillis(), window.getEndTimeMillis(), true);
        final List<DashboardUsageAggregation> buckets =
            getBuckets(context.getOperationContext(), urnFilter, urn, timeseriesAspectService);
        for (Key key : entry.getValue()) {
          resultsByKey.put(key, buckets);
        }
        continue;
      }

      // URN-free filter; batchGetAggregatedStats adds the `urn IN [...]` criterion itself.
      final Filter sharedFilter =
          buildSharedUsageFilter(window.getStartTimeMillis(), window.getEndTimeMillis(), true);

      final Map<Urn, GenericTable> batchResult =
          timeseriesAspectService.batchGetAggregatedStats(
              context.getOperationContext(),
              Constants.DASHBOARD_ENTITY_NAME,
              Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
              getUsageBucketAggSpecs(),
              urns,
              sharedFilter,
              getDailyUsageGroupingBuckets());

      for (Key key : entry.getValue()) {
        final Urn urn = UrnUtils.getUrn(key.getUrn());
        final GenericTable table = batchResult.get(urn);
        resultsByKey.put(
            key, table == null ? new ArrayList<>() : mapUsageAggregations(table, key.getUrn()));
      }
    }

    // Return in the exact order of the requested keys, as the DataLoader contract requires.
    return keys.stream().map(resultsByKey::get).collect(Collectors.toList());
  }

  @Value
  private static class TimeWindow {
    @Nullable Long startTimeMillis;
    @Nullable Long endTimeMillis;
  }
}
