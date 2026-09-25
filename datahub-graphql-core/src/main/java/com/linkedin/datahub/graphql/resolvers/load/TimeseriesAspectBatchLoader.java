package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * DataLoader for batching timeseries aspect reads across multiple entities. When a single URN is
 * dispatched, or when {@code limit} is null, the single-URN {@link
 * TimeseriesAspectService#getAspectValues} path is used to avoid aggregation overhead. For batches
 * of two or more URNs with uniform parameters, {@link TimeseriesAspectService#batchGetAspectValues}
 * fires one top_hits aggregation query per group.
 */
@Slf4j
@RequiredArgsConstructor
public class TimeseriesAspectBatchLoader {

  public static final String LOADER_NAME = "TimeseriesAspect";

  private final TimeseriesAspectService timeseriesAspectService;

  public static DataLoader<Key, List<EnvelopedAspect>> createDataLoader(
      final TimeseriesAspectService timeseriesAspectService, final QueryContext queryContext) {
    final TimeseriesAspectBatchLoader loader =
        new TimeseriesAspectBatchLoader(timeseriesAspectService);
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

  /**
   * Resolves a list of {@link Key}s. Keys are grouped by their uniform parameters; each group is
   * served by a single ES query when {@code size > 1}, or by a direct per-URN call when {@code size
   * == 1} or {@code limit} is null.
   */
  public List<List<EnvelopedAspect>> batchLoad(final List<Key> keys, final QueryContext context) {
    final List<List<EnvelopedAspect>> results =
        new ArrayList<>(Collections.nCopies(keys.size(), null));

    final Map<GroupKey, List<Integer>> groups = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      final Key k = keys.get(i);
      groups
          .computeIfAbsent(
              new GroupKey(
                  k.getEntityName(),
                  k.getAspectName(),
                  k.getStartTimeMillis(),
                  k.getEndTimeMillis(),
                  k.getLimit(),
                  k.getSharedFilter(),
                  k.getSort()),
              g -> new ArrayList<>())
          .add(i);
    }

    for (Map.Entry<GroupKey, List<Integer>> entry : groups.entrySet()) {
      final GroupKey gk = entry.getKey();
      final List<Integer> indices = entry.getValue();

      if (indices.size() == 1 || gk.getLimit() == null) {
        // Single URN → no aggregation benefit. Null limit → caller wants the configured
        // API default (up to 5000 docs per URN), which exceeds top_hits.size's practical
        // cap (index.max_inner_result_window, default 100). Both cases use the direct path.
        for (int idx : indices) {
          final Key k = keys.get(idx);
          results.set(
              idx,
              timeseriesAspectService.getAspectValues(
                  context.getOperationContext(),
                  UrnUtils.getUrn(k.getUrn()),
                  k.getEntityName(),
                  k.getAspectName(),
                  k.getStartTimeMillis(),
                  k.getEndTimeMillis(),
                  k.getLimit(),
                  k.getSharedFilter(),
                  k.getSort()));
        }
      } else {
        // Multiple URNs with uniform parameters — one top_hits agg query.
        final Set<Urn> urns = new HashSet<>();
        for (int idx : indices) {
          urns.add(UrnUtils.getUrn(keys.get(idx).getUrn()));
        }
        final Map<Urn, List<EnvelopedAspect>> batchResult =
            timeseriesAspectService.batchGetAspectValues(
                context.getOperationContext(),
                urns,
                gk.getEntityName(),
                gk.getAspectName(),
                gk.getStartTimeMillis(),
                gk.getEndTimeMillis(),
                gk.getLimit(),
                gk.getSharedFilter(),
                gk.getSort());
        for (int idx : indices) {
          final Urn urn = UrnUtils.getUrn(keys.get(idx).getUrn());
          results.set(idx, batchResult.getOrDefault(urn, Collections.emptyList()));
        }
      }
    }

    return results;
  }

  /** Per-entity key encoding all parameters that determine which ES documents to fetch. */
  @Value
  public static class Key {
    String urn;
    String entityName;
    String aspectName;
    @Nullable Long startTimeMillis;
    @Nullable Long endTimeMillis;

    /**
     * Null means the caller wants the configured API default (up to 5000 docs). That exceeds {@code
     * top_hits.size}'s practical cap, so null limit always uses the single-URN path. Non-null
     * values must be small enough to fit within {@code index.max_inner_result_window} (OpenSearch
     * default: 100).
     */
    @Nullable Integer limit;

    @Nullable Filter sharedFilter;
    @Nullable SortCriterion sort;
  }

  /** Parameters shared across all URNs in a batch group; must be identical to batch-merge. */
  @Value
  private static class GroupKey {
    String entityName;
    String aspectName;
    @Nullable Long startTimeMillis;
    @Nullable Long endTimeMillis;
    @Nullable Integer limit;
    @Nullable Filter sharedFilter;
    @Nullable SortCriterion sort;
  }
}
