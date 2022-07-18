package com.linkedin.datahub.graphql.resolvers.dashboard;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.generated.DashboardStatsSummary;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.features.UsageFeatures;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;

@Slf4j
public class DashboardStatsSummaryResolver implements DataFetcher<CompletableFuture<DashboardStatsSummary>> {

  // The maximum number of top users to show in the summary stats
  private static final Integer MAX_TOP_USERS = 5;

  private final EntityClient entityClient;
  private final TimeseriesAspectService timeseriesAspectService;
  private final Cache<Urn, DashboardStatsSummary> summaryCache;

  public DashboardStatsSummaryResolver(final EntityClient entityClient, final TimeseriesAspectService timeseriesAspectService) {
    this.entityClient = entityClient;
    this.timeseriesAspectService = timeseriesAspectService;
    this.summaryCache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(6, TimeUnit.HOURS) // TODO: Make caching duration configurable externally.
        .build();
  }

  @Override
  public CompletableFuture<DashboardStatsSummary> get(DataFetchingEnvironment environment) throws Exception {
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (this.summaryCache.getIfPresent(resourceUrn) != null) {
        return this.summaryCache.getIfPresent(resourceUrn);
      }

      try {

        // acryl-main only - first see if we can populate stats based on the UsageFeatures aspect
        UsageFeatures maybeUsageFeatures = getUsageFeatures(resourceUrn, context);
        if (maybeUsageFeatures != null) {
          // Do not cache to ensure we're up to date.
          return getSummaryFromUsageFeatures(maybeUsageFeatures);
        }

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

  @Nullable
  private UsageFeatures getUsageFeatures(final Urn datasetUrn, final QueryContext context) {
    try {
      EntityResponse response = this.entityClient.getV2(
          Constants.DASHBOARD_ENTITY_NAME,
          datasetUrn,
          ImmutableSet.of(Constants.USAGE_FEATURES_ASPECT_NAME),
          context.getAuthentication());

      if (response != null && response.getAspects().containsKey(Constants.USAGE_FEATURES_ASPECT_NAME)) {
        return new UsageFeatures(response.getAspects().get(Constants.USAGE_FEATURES_ASPECT_NAME).getValue().data());
      } else {
        // No usage features found for urn.
        return null;
      }

    } catch (Exception e) {
      log.error(String.format("Failed to retrieve usage features aspect for dataset urn %s. Returning null...", datasetUrn));
      return null;
    }
  }

  /**
   * Saas-Only: Generates a Dataset Stats Summary using the UsageFeatures aspect which computed
   * asynchronously and stored in GMS. This helps to reduce computation latency on the read side.
   */
  private DashboardStatsSummary getSummaryFromUsageFeatures(@Nonnull final UsageFeatures usageFeatures) {
    final DashboardStatsSummary result = new DashboardStatsSummary();
    // View stats
    if (usageFeatures.hasViewCountTotal()) {
      result.setViewCount(usageFeatures.getViewCountTotal().intValue());
    }
    if (usageFeatures.hasViewCountLast30Days()) {
      result.setViewCountLast30Days(usageFeatures.getViewCountLast30Days().intValue());
    }
    if (usageFeatures.hasViewCountPercentileLast30Days()) {
      result.setViewCountPercentileLast30Days(usageFeatures.getViewCountPercentileLast30Days());
    }

    // User stats
    if (usageFeatures.hasUniqueUserCountLast30Days()) {
      result.setUniqueUserCountLast30Days(usageFeatures.getUniqueUserCountLast30Days().intValue());
    }
    if (usageFeatures.hasUniqueUserPercentileLast30Days()) {
      result.setUniqueUserPercentileLast30Days(usageFeatures.getUniqueUserPercentileLast30Days());
    }
    if (usageFeatures.hasUniqueUserRankLast30Days()) {
      result.setUniqueUserRankLast30Days(usageFeatures.getUniqueUserRankLast30Days().intValue());
    }

    // Top users
    if (usageFeatures.hasTopUsersLast30Days()) {
      result.setTopUsersLast30Days(trimUsers(usageFeatures.getTopUsersLast30Days()
          .stream()
          .map(userUrn -> createPartialUser(Objects.requireNonNull(userUrn)))
          .collect(Collectors.toList())));
    }
    return result;
  }

  private CorpUser createPartialUser(final Urn userUrn) {
    final CorpUser result = new CorpUser();
    result.setUrn(userUrn.toString());
    return result;
  }
 }
