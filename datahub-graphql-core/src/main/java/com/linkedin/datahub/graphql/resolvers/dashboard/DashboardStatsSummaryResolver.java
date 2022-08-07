package com.linkedin.datahub.graphql.resolvers.dashboard;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.generated.DashboardStatsSummary;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;

@Slf4j
public class DashboardStatsSummaryResolver implements DataFetcher<CompletableFuture<DashboardStatsSummary>> {

  // The maximum number of top users to show in the summary stats
  private static final Integer MAX_TOP_USERS = 5;

  private final TimeseriesAspectService timeseriesAspectService;
  private final Cache<Urn, DashboardStatsSummary> summaryCache;

  public DashboardStatsSummaryResolver(final TimeseriesAspectService timeseriesAspectService) {
    this.timeseriesAspectService = timeseriesAspectService;
    this.summaryCache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(6, TimeUnit.HOURS) // TODO: Make caching duration configurable externally.
        .build();
  }

  @Override
  public CompletableFuture<DashboardStatsSummary> get(DataFetchingEnvironment environment) throws Exception {
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return CompletableFuture.supplyAsync(() -> {

      if (this.summaryCache.getIfPresent(resourceUrn) != null) {
        return this.summaryCache.getIfPresent(resourceUrn);
      }

      try {

        final DashboardStatsSummary result = new DashboardStatsSummary();

        // Obtain total dashboard view count, by viewing the latest reported dashboard metrics.
        List<DashboardUsageMetrics> dashboardUsageMetrics =
            getDashboardUsageMetrics(resourceUrn.toString(), null, null, 1, this.timeseriesAspectService);
        if (dashboardUsageMetrics.size() > 0) {
          result.setViewCount(getDashboardViewCount(resourceUrn));
        }

        // Obtain unique user statistics, by rolling up unique users over the past month.
        List<DashboardUserUsageCounts> userUsageCounts = getDashboardUsagePerUser(resourceUrn);
        result.setUniqueUserCountLast30Days(userUsageCounts.size());
        result.setTopUsersLast30Days(
            trimUsers(userUsageCounts.stream().map(DashboardUserUsageCounts::getUser).collect(Collectors.toList())));

        this.summaryCache.put(resourceUrn, result);
        return result;

      } catch (Exception e) {
        log.error(String.format("Failed to load dashboard usage summary for resource %s", resourceUrn.toString()), e);
        return null; // Do not throw when loading usage summary fails.
      }
    });
  }

  private int getDashboardViewCount(final Urn resourceUrn) {
    List<DashboardUsageMetrics> dashboardUsageMetrics = getDashboardUsageMetrics(
        resourceUrn.toString(),
        null,
        null,
        1,
        this.timeseriesAspectService);
    return dashboardUsageMetrics.get(0).getViewsCount();
  }

  private List<DashboardUserUsageCounts> getDashboardUsagePerUser(final Urn resourceUrn) {
    long now = System.currentTimeMillis();
    long nowMinusOneMonth = timeMinusOneMonth(now);
    Filter bucketStatsFilter = createUsageFilter(resourceUrn.toString(), nowMinusOneMonth, now, true);
    return getUserUsageCounts(bucketStatsFilter, this.timeseriesAspectService);
  }

  private List<CorpUser> trimUsers(final List<CorpUser> originalUsers) {
    if (originalUsers.size() > MAX_TOP_USERS) {
      return originalUsers.subList(0, MAX_TOP_USERS);
    }
    return originalUsers;
  }
 }
