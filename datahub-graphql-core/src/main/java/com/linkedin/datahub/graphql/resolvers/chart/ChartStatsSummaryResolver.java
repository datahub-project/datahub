package com.linkedin.datahub.graphql.resolvers.chart;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ChartStatsSummary;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
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



@Slf4j
public class ChartStatsSummaryResolver implements DataFetcher<CompletableFuture<ChartStatsSummary>> {

  // The maximum number of top users to show in the summary stats
  private static final Integer MAX_TOP_USERS = 5;

  private final EntityClient entityClient;
  private final TimeseriesAspectService timeseriesAspectService;
  private final Cache<Urn, ChartStatsSummary> summaryCache;

  public ChartStatsSummaryResolver(final EntityClient entityClient, final TimeseriesAspectService timeseriesAspectService) {
    this.entityClient = entityClient;
    this.timeseriesAspectService = timeseriesAspectService;
    this.summaryCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(6, TimeUnit.HOURS).build();
  }

  @Override
  public CompletableFuture<ChartStatsSummary> get(DataFetchingEnvironment environment) throws Exception {
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (this.summaryCache.getIfPresent(resourceUrn) != null) {
        return this.summaryCache.getIfPresent(resourceUrn);
      }

      try {
        // acryl-main only - first see if we can populate stats based on the UsageFeatures aspect
        UsageFeatures maybeUsageFeatures = getUsageFeatures(resourceUrn, context);
        return getSummaryFromUsageFeatures(maybeUsageFeatures);
      } catch (Exception e) {
        log.error(String.format("Failed to load dashboard usage summary for resource %s", resourceUrn.toString()), e);
        return null; // Do not throw when loading usage summary fails.
      }
    });
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
      EntityResponse response = this.entityClient.getV2(Constants.DASHBOARD_ENTITY_NAME, datasetUrn,
          ImmutableSet.of(Constants.USAGE_FEATURES_ASPECT_NAME), context.getAuthentication());

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
   * Saas-Only: Generates a Chart Stats Summary using the UsageFeatures aspect which computed
   * asynchronously and stored in GMS. This helps to reduce computation latency on the read side.
   */
  private ChartStatsSummary getSummaryFromUsageFeatures(@Nonnull final UsageFeatures usageFeatures) {
    final ChartStatsSummary result = new ChartStatsSummary();
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