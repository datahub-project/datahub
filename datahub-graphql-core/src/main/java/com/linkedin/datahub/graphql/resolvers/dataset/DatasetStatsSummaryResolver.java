package com.linkedin.datahub.graphql.resolvers.dataset;

import com.datahub.authorization.EntitySpec;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.metadata.authorization.PoliciesConfig;
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

  private final UsageClient usageClient;
  private final Cache<Urn, DatasetStatsSummary> summaryCache;

  public DatasetStatsSummaryResolver(final UsageClient usageClient) {
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

            if (!isAuthorized(resourceUrn, context)) {
              log.debug(
                  "User {} is not authorized to view profile information for dataset {}",
                  context.getActorUrn(),
                  resourceUrn.toString());
              return null;
            }

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
            this.summaryCache.put(resourceUrn, result);
            return result;
          } catch (Exception e) {
            log.error(
                String.format(
                    "Failed to load Usage Stats summary for resource %s", resourceUrn.toString()),
                e);
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

  private CorpUser createPartialUser(final Urn userUrn) {
    final CorpUser result = new CorpUser();
    result.setUrn(userUrn.toString());
    return result;
  }

  private boolean isAuthorized(final Urn resourceUrn, final QueryContext context) {
    return AuthorizationUtils.isAuthorized(
        context,
        Optional.of(new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString())),
        PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE);
  }
}
