package com.linkedin.datahub.graphql.resolvers.dataset;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.metadata.client.UsageStatsJavaClient;
import com.linkedin.usage.UsageTimeRange;
import com.linkedin.usage.UserUsageCounts;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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

  private final UsageStatsJavaClient usageClient;

  public DatasetStatsSummaryResolver(final UsageStatsJavaClient usageClient) {
    this.usageClient = usageClient;
  }

  @Override
  public CompletableFuture<DatasetStatsSummary> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.isViewDatasetUsageAuthorized(context, resourceUrn)) {
              log.debug(
                  "User {} is not authorized to view profile information for dataset {}",
                  context.getActorUrn(),
                  resourceUrn);
              return null;
            }

            com.linkedin.usage.UsageQueryResult usageQueryResult =
                usageClient.getUsageStats(
                    context.getOperationContext(), resourceUrn.toString(), UsageTimeRange.MONTH);

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

            return result;
          } catch (Exception e) {
            log.error(
                String.format(
                    "Failed to load Usage Stats summary for resource %s", resourceUrn.toString()),
                e);
            return null; // Do not throw when loading usage summary fails.
          }
        },
        this.getClass().getSimpleName(),
        "get");
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
}
