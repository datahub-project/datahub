package com.linkedin.datahub.graphql.resolvers.dataset;

import com.datahub.authorization.EntitySpec;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.search.features.StorageFeatures;
import com.linkedin.metadata.search.features.UsageFeatures;
import com.linkedin.usage.UsageClient;
import com.linkedin.usage.UsageTimeRange;
import com.linkedin.usage.UserUsageCounts;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This resolver is a thin wrapper around the {@link DatasetUsageStatsResolver} which simply
 * computes some aggregate usage metrics for a Dashboard.
 */
@Slf4j
public class DatasetStatsSummaryResolver
    implements DataFetcher<CompletableFuture<DatasetStatsSummary>> {

  // The maximum number of top users to show in the summary stats
  private static final Integer MAX_TOP_USERS = 5;

  private final EntityClient entityClient;
  private final UsageClient usageClient;
  private final Cache<Urn, DatasetStatsSummary> summaryCache;

  public DatasetStatsSummaryResolver(
      final EntityClient entityClient, final UsageClient usageClient) {
    this.entityClient = entityClient;
    this.usageClient = usageClient;
    this.summaryCache =
        CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(
                6, TimeUnit.HOURS) // TODO: Make caching duration configurable externally.
            .build();
  }

  @Override
  public CompletableFuture<DatasetStatsSummary> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          if (this.summaryCache.getIfPresent(resourceUrn) != null) {
            return this.summaryCache.getIfPresent(resourceUrn);
          }

          try {
<<<<<<< HEAD
=======

>>>>>>> oss_master
            if (!isAuthorized(resourceUrn, context)) {
              log.debug(
                  "User {} is not authorized to view profile information for dataset {}",
                  context.getActorUrn(),
                  resourceUrn.toString());
              return null;
            }
<<<<<<< HEAD
            final EntityResponse response = getOfflineFeatures(resourceUrn, context);

            final UsageFeatures maybeUsageFeatures =
                response != null
                        && response.getAspects().containsKey(Constants.USAGE_FEATURES_ASPECT_NAME)
                    ? new UsageFeatures(
                        response
                            .getAspects()
                            .get(Constants.USAGE_FEATURES_ASPECT_NAME)
                            .getValue()
                            .data())
                    : null;
            final StorageFeatures maybeStorageFeatures =
                response != null
                        && response.getAspects().containsKey(Constants.STORAGE_FEATURES_ASPECT_NAME)
                    ? new StorageFeatures(
                        response
                            .getAspects()
                            .get(Constants.STORAGE_FEATURES_ASPECT_NAME)
                            .getValue()
                            .data())
                    : null;

            com.linkedin.usage.UsageQueryResult usageQueryResult =
                usageClient.getUsageStats(resourceUrn.toString(), UsageTimeRange.MONTH);

            final DatasetStatsSummary result = new DatasetStatsSummary();
            addUsageFeatures(result, maybeUsageFeatures, resourceUrn, context);
            addStorageFeatures(result, maybeStorageFeatures);

            // acryl-main only - first see if we can populate stats based on the UsageFeatures
            // aspect
=======

            com.linkedin.usage.UsageQueryResult usageQueryResult =
                usageClient.getUsageStats(resourceUrn.toString(), UsageTimeRange.MONTH);

            final DatasetStatsSummary result = new DatasetStatsSummary();
            result.setQueryCountLast30Days(usageQueryResult.getAggregations().getTotalSqlQueries());
            result.setUniqueUserCountLast30Days(
                usageQueryResult.getAggregations().getUniqueUserCount());
            if (usageQueryResult.getAggregations().hasUsers()) {
              result.setTopUsersLast30Days(
                  trimUsers(
                      usageQueryResult.getAggregations().getUsers().stream()
                          .filter(UserUsageCounts::hasUser)
                          .sorted((a, b) -> (b.getCount() - a.getCount()))
                          .map(
                              userCounts ->
                                  createPartialUser(Objects.requireNonNull(userCounts.getUser())))
                          .collect(Collectors.toList())));
            }
>>>>>>> oss_master
            this.summaryCache.put(resourceUrn, result);
            return result;
          } catch (Exception e) {
            log.error(
                String.format(
<<<<<<< HEAD
                    "Failed to load Stats summary for resource %s", resourceUrn.toString()),
=======
                    "Failed to load Usage Stats summary for resource %s", resourceUrn.toString()),
>>>>>>> oss_master
                e);
            return null; // Do not throw when loading usage summary fails.
          }
        });
<<<<<<< HEAD
  }

  private void addUsageFeatures(
      @Nonnull final DatasetStatsSummary result,
      @Nullable final UsageFeatures maybeUsageFeatures,
      @Nonnull final Urn resourceUrn,
      @Nonnull final QueryContext context)
      throws Exception {
    // If we have offline-computed usage features, use those to avoid the expensive query.
    if (maybeUsageFeatures != null) {
      // Do not cache to ensure we're up to date.
      addSummaryFromOfflineUsageFeatures(maybeUsageFeatures, result);
      return;
    }

    // else compute usage features normally
    com.linkedin.usage.UsageQueryResult usageQueryResult =
        usageClient.getUsageStats(resourceUrn.toString(), UsageTimeRange.MONTH);
    result.setQueryCountLast30Days(usageQueryResult.getAggregations().getTotalSqlQueries());
    result.setUniqueUserCountLast30Days(usageQueryResult.getAggregations().getUniqueUserCount());
    if (usageQueryResult.getAggregations().hasUsers()) {
      result.setTopUsersLast30Days(
          trimUsers(
              usageQueryResult.getAggregations().getUsers().stream()
                  .filter(UserUsageCounts::hasUser)
                  .sorted((a, b) -> (b.getCount() - a.getCount()))
                  .map(
                      userCounts -> createPartialUser(Objects.requireNonNull(userCounts.getUser())))
                  .collect(Collectors.toList())));
    }
  }

  private void addStorageFeatures(
      @Nonnull final DatasetStatsSummary result,
      @Nullable final StorageFeatures maybeStorageFeatures)
      throws Exception {
    // We only add storage features if we have them.
    if (maybeStorageFeatures != null) {
      // Do not cache to ensure we're up to date.
      addSummaryFromOfflineStorageFeatures(maybeStorageFeatures, result);
    }
=======
>>>>>>> oss_master
  }

  private List<CorpUser> trimUsers(final List<CorpUser> originalUsers) {
    if (originalUsers.size() > MAX_TOP_USERS) {
      return originalUsers.subList(0, MAX_TOP_USERS);
    }
    return originalUsers;
  }

  private CorpUser createPartialUser(final Urn userUrn) {
    final CorpUser result = new CorpUser();
    result.setUrn(userUrn.toString());
    return result;
  }

  @Nullable
  private EntityResponse getOfflineFeatures(final Urn datasetUrn, final QueryContext context) {
    try {
      return this.entityClient.getV2(
          Constants.DATASET_ENTITY_NAME,
          datasetUrn,
          ImmutableSet.of(
              Constants.USAGE_FEATURES_ASPECT_NAME, Constants.STORAGE_FEATURES_ASPECT_NAME),
          context.getAuthentication());
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to retrieve usage features aspect for dataset urn %s. Returning null...",
              datasetUrn));
      return null;
    }
  }

  /**
   * Saas-Only: Adds to Dataset Stats Summary using the UsageFeatures aspect which computed
   * asynchronously and stored in GMS. This helps to reduce computation latency on the read side.
   */
  private void addSummaryFromOfflineUsageFeatures(
      @Nonnull final UsageFeatures usageFeatures, @Nullable final DatasetStatsSummary result) {

    // Query stats
    if (usageFeatures.hasUsageCountLast30Days()) {
      result.setQueryCountLast30Days(usageFeatures.getUsageCountLast30Days().intValue());
    }
    if (usageFeatures.hasQueryCountPercentileLast30Days()) {
      result.setQueryCountPercentileLast30Days(usageFeatures.getQueryCountPercentileLast30Days());
    }
    if (usageFeatures.hasQueryCountRankLast30Days()) {
      result.setQueryCountRankLast30Days(usageFeatures.getQueryCountRankLast30Days().intValue());
    }
    if (usageFeatures.hasWriteCountLast30Days()) {
      result.setUpdateCountLast30Days(usageFeatures.getWriteCountLast30Days().intValue());
    }
    if (usageFeatures.hasWriteCountPercentileLast30Days()) {
      result.setUpdateCountPercentileLast30Days(usageFeatures.getWriteCountPercentileLast30Days());
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
      result.setTopUsersLast30Days(
          trimUsers(
              usageFeatures.getTopUsersLast30Days().stream()
                  .map(userUrn -> createPartialUser(Objects.requireNonNull(userUrn)))
                  .collect(Collectors.toList())));
    }
  }

  /**
   * Saas-Only: Adds to Dataset Stats Summary using the StorageFeatures aspect which computed
   * asynchronously and stored in GMS. This helps to reduce computation latency on the read side.
   */
  private void addSummaryFromOfflineStorageFeatures(
      @Nonnull final StorageFeatures storageFeatures, @Nullable final DatasetStatsSummary result) {
    // Query stats
    if (storageFeatures.hasRowCount()) {
      result.setRowCount(storageFeatures.getRowCount());
    }
    if (storageFeatures.hasRowCountPercentile()) {
      result.setRowCountPercentile(storageFeatures.getRowCountPercentile());
    }
    if (storageFeatures.hasSizeInBytes()) {
      result.setSizeInBytes(storageFeatures.getSizeInBytes());
    }
    if (storageFeatures.hasSizeInBytesPercentile()) {
      result.setSizeInBytesPercentile(storageFeatures.getSizeInBytesPercentile().intValue());
    }
  }

  private boolean isAuthorized(final Urn resourceUrn, final QueryContext context) {
    return AuthorizationUtils.isAuthorized(
        context,
        Optional.of(new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString())),
        PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE);
  }
}
