package com.linkedin.datahub.graphql.resolvers.load;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.TimeseriesUtils;
import com.linkedin.metadata.timeseries.elastic.UsageServiceUtil;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.usage.UsageTimeRange;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * DataLoader that batches {@code Dataset.statsSummary} across every dataset in a single GraphQL
 * request, replacing the per-dataset fan-out in {@link
 * com.linkedin.datahub.graphql.resolvers.dataset.DatasetStatsSummaryResolver} (which calls the
 * usage client once per dataset → multiple ES aggregations each).
 *
 * <p>The summary only needs three values from the last 30 days, so this fires just two batched
 * {@code getAggregatedStats} queries over the {@code datasetUsageStatistics} timeseries aspect (one
 * outer-terms-bucket ES query per sub-batch), keyed on the default {@code urn} field:
 *
 * <ol>
 *   <li><b>queryCount</b> — DATE(day) bucket + LATEST {@code totalSqlQueries}; the per-day latest
 *       values are summed per dataset (mirrors {@code UsageServiceUtil.query} step 4).
 *   <li><b>uniqueUserCount + topUsers</b> — STRING bucket on {@code userCounts.user} + SUM {@code
 *       userCounts.count}; the number of user buckets is the unique-user count and the top {@value
 *       #MAX_TOP_USERS} by summed count are the top users.
 * </ol>
 *
 * <p>The aggregation specs, grouping buckets, time window, and shared filter all come from {@link
 * UsageServiceUtil}/{@link TimeseriesUtils} — the same definitions the per-URN path uses — so the
 * batched and per-entity results stay identical. The 30-day window is constant (the resolver always
 * requests {@code MONTH}), so the DataLoader key is just the dataset {@link Urn}.
 */
@Slf4j
@RequiredArgsConstructor
public class DatasetStatsSummaryBatchLoader {

  public static final String LOADER_NAME = "DatasetStatsSummary";

  private static final int MAX_TOP_USERS = 5;
  // Sentinel the timeseries aggregation returns for absent metric values.
  private static final String ES_NULL_VALUE = "NULL";

  // statsSummary is a coarse 30-day rollup and a non-critical display stat, so a short 1-hour TTL
  // is enough to dedupe repeat/concurrent views without risking noticeably stale numbers. Bounded
  // to a small entry count so the cache's memory footprint stays small (~1-2 MB).
  private static final Duration CACHE_TTL = Duration.ofHours(1);
  private static final long CACHE_MAX_SIZE = 1_000;

  // Process-wide shared cache used by the production DataLoader (createDataLoader). Declared after
  // the TTL/size constants so they are initialized first. Tests inject their own cache instead.
  private static final Cache<Urn, DatasetStatsSummary> CACHE = newCache();

  private final TimeseriesAspectService timeseriesAspectService;
  // The cache backing this loader instance. In production every instance shares the process-wide
  // CACHE (injected via createDataLoader); tests inject a fresh cache. The DatasetStatsSummary
  // values are viewer-independent — per-URN authorization is enforced (below) before any URN's data
  // is read from the cache or ES — so keying on Urn alone is safe.
  private final Cache<Urn, DatasetStatsSummary> cache;
  // Per-URN view-usage authorization; injected for testability. Production uses
  // AuthorizationUtils::isViewDatasetUsageAuthorized.
  private final BiPredicate<QueryContext, Urn> viewAuthorization;

  /** Builds a summary cache (Caffeine). Used for the process-wide {@link #CACHE} and by tests. */
  public static Cache<Urn, DatasetStatsSummary> newCache() {
    return Caffeine.newBuilder().expireAfterWrite(CACHE_TTL).maximumSize(CACHE_MAX_SIZE).build();
  }

  public static DataLoader<Urn, DatasetStatsSummary> createDataLoader(
      final TimeseriesAspectService timeseriesAspectService, final QueryContext queryContext) {
    final DatasetStatsSummaryBatchLoader loader =
        new DatasetStatsSummaryBatchLoader(
            timeseriesAspectService, CACHE, AuthorizationUtils::isViewDatasetUsageAuthorized);
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

  public List<DatasetStatsSummary> batchLoad(final List<Urn> urns, final QueryContext context) {
    // Authorize each URN in parallel, off graphql-java's field-completion path. Unauthorized URNs
    // are never fetched and resolve to null — same result as the per-URN resolver, but the policy
    // evaluations run concurrently instead of blocking list completion one field at a time.
    final Set<Urn> authorized = authorizeInParallel(urns, context);

    // Serve cache hits directly; only authorized misses reach ES (batched below). This keeps the
    // batching win on the miss set while regaining the per-URN cache's repeat-view speed.
    final Map<Urn, DatasetStatsSummary> resolved = new HashMap<>();
    final List<Urn> misses = new ArrayList<>();
    for (Urn urn : urns) {
      if (!authorized.contains(urn)) {
        continue; // unauthorized → left null in the result
      }
      final DatasetStatsSummary cached = cache.getIfPresent(urn);
      if (cached != null) {
        resolved.put(urn, cached);
      } else {
        misses.add(urn);
      }
    }

    if (!misses.isEmpty()) {
      final OperationContext opContext = context.getOperationContext();
      final long now = System.currentTimeMillis();
      // Same window the per-URN resolver requests (UsageTimeRange.MONTH), sans per-URN criterion.
      final long startMillis = TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.MONTH, now);
      final Filter sharedFilter = timeWindowFilter(startMillis, now);

      // A: per-day LATEST totalSqlQueries → summed per dataset = queryCountLast30Days.
      final CompletableFuture<Map<Urn, GenericTable>> queryCountFuture =
          GraphQLConcurrencyUtils.supplyAsync(
              () ->
                  timeseriesAspectService.batchGetAggregatedStats(
                      opContext,
                      UsageServiceUtil.USAGE_STATS_ENTITY_NAME,
                      UsageServiceUtil.USAGE_STATS_ASPECT_NAME,
                      new AggregationSpec[] {
                        UsageServiceUtil.latestAggSpec(UsageServiceUtil.FIELD_TOTAL_SQL_QUERIES)
                      },
                      misses,
                      sharedFilter,
                      new GroupingBucket[] {
                        UsageServiceUtil.usageTimestampBucket(WindowDuration.DAY, null)
                      }),
              LOADER_NAME,
              "batchGetAggregatedStats:queryCount");

      // B: SUM userCounts.count grouped by userCounts.user → uniqueUserCount + topUsers.
      final CompletableFuture<Map<Urn, GenericTable>> userCountsFuture =
          GraphQLConcurrencyUtils.supplyAsync(
              () ->
                  timeseriesAspectService.batchGetAggregatedStats(
                      opContext,
                      UsageServiceUtil.USAGE_STATS_ENTITY_NAME,
                      UsageServiceUtil.USAGE_STATS_ASPECT_NAME,
                      new AggregationSpec[] {
                        UsageServiceUtil.sumAggSpec(UsageServiceUtil.FIELD_USER_COUNTS_COUNT)
                      },
                      misses,
                      sharedFilter,
                      new GroupingBucket[] {UsageServiceUtil.userCountsGroupingBucket()}),
              LOADER_NAME,
              "batchGetAggregatedStats:userCounts");

      CompletableFuture.allOf(queryCountFuture, userCountsFuture).join();
      final Map<Urn, GenericTable> queryCounts = queryCountFuture.join();
      final Map<Urn, GenericTable> userCounts = userCountsFuture.join();

      for (Urn urn : misses) {
        final DatasetStatsSummary summary = buildSummary(queryCounts.get(urn), userCounts.get(urn));
        cache.put(urn, summary);
        resolved.put(urn, summary);
      }
    }

    final List<DatasetStatsSummary> results = new ArrayList<>(urns.size());
    for (Urn urn : urns) {
      results.add(resolved.get(urn));
    }
    return results;
  }

  /**
   * Runs the per-URN view-usage authorization checks concurrently on the GraphQL executor and
   * returns the URNs the caller may view. Each check is a CPU-bound policy evaluation under a
   * shared read lock, so it parallelizes cleanly; a check that throws is treated as unauthorized.
   */
  private Set<Urn> authorizeInParallel(final List<Urn> urns, final QueryContext context) {
    final List<CompletableFuture<Urn>> futures =
        urns.stream()
            .distinct()
            .map(
                urn ->
                    GraphQLConcurrencyUtils.supplyAsync(
                        () -> {
                          try {
                            return viewAuthorization.test(context, urn) ? urn : null;
                          } catch (Exception e) {
                            log.warn(
                                "Authorization check failed for {}; treating as unauthorized",
                                urn,
                                e);
                            return null;
                          }
                        },
                        LOADER_NAME,
                        "authorize"))
            .collect(Collectors.toList());
    final Set<Urn> authorized = new HashSet<>();
    for (CompletableFuture<Urn> future : futures) {
      final Urn urn = future.join();
      if (urn != null) {
        authorized.add(urn);
      }
    }
    return authorized;
  }

  private DatasetStatsSummary buildSummary(
      final GenericTable queryCountTable, final GenericTable userCountsTable) {
    final DatasetStatsSummary summary = new DatasetStatsSummary();

    // queryCountLast30Days: sum of per-day latest totalSqlQueries (col 1), skipping NULL/absent.
    if (queryCountTable != null && queryCountTable.hasRows()) {
      Integer total = null;
      for (StringArrayRow row : rows(queryCountTable)) {
        final Integer perDay = parseIntOrNull(row.get(1));
        if (perDay != null) {
          total = (total == null ? 0 : total) + perDay;
        }
      }
      if (total != null) {
        summary.setQueryCountLast30Days(total);
      }
    }

    // uniqueUserCountLast30Days + topUsersLast30Days from the per-user rows [userUrn, sumCount].
    if (userCountsTable != null && userCountsTable.hasRows()) {
      final List<StringArrayRow> userRows = rows(userCountsTable);
      summary.setUniqueUserCountLast30Days(userRows.size());
      final List<CorpUser> topUsers =
          userRows.stream()
              .filter(r -> !isNull(r.get(0)))
              // Count desc, then user urn asc as a deterministic tie-break so the top-N is stable
              // and matches the per-URN path (DatasetStatsSummaryResolver).
              .sorted(
                  Comparator.comparingInt((StringArrayRow r) -> orZero(parseIntOrNull(r.get(1))))
                      .reversed()
                      .thenComparing(r -> r.get(0)))
              .limit(MAX_TOP_USERS)
              .map(r -> partialUser(r.get(0)))
              .collect(Collectors.toList());
      if (!topUsers.isEmpty()) {
        summary.setTopUsersLast30Days(topUsers);
      }
    }

    return summary;
  }

  /**
   * Time-window-only filter (no per-URN criterion — the batch method adds {@code urn IN [urns]}).
   */
  private static Filter timeWindowFilter(final long startMillis, final long endMillis) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            TimeseriesUtils.createTimeRangeCriteria(startMillis, endMillis)))));
  }

  private static CorpUser partialUser(final String urn) {
    final CorpUser user = new CorpUser();
    user.setUrn(urn);
    return user;
  }

  private static boolean isNull(final String value) {
    return value == null || ES_NULL_VALUE.equals(value);
  }

  private static Integer parseIntOrNull(final String value) {
    if (isNull(value)) {
      return null;
    }
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static int orZero(final Integer value) {
    return value == null ? 0 : value;
  }

  private static List<StringArrayRow> rows(final GenericTable table) {
    if (table.getRows() == null) {
      return Collections.emptyList();
    }
    final List<StringArrayRow> out = new ArrayList<>(table.getRows().size());
    table.getRows().forEach(r -> out.add(new StringArrayRow(r)));
    return out;
  }

  /** Thin adapter so row access reads clearly and stays bounds-safe. */
  @Value
  private static class StringArrayRow {
    com.linkedin.data.template.StringArray raw;

    String get(final int i) {
      return i < raw.size() ? raw.get(i) : null;
    }
  }
}
