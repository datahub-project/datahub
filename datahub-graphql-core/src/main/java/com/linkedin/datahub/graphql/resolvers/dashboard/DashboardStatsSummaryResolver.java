package com.linkedin.datahub.graphql.resolvers.dashboard;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.isViewDatasetUsageAuthorized;
import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DashboardStatsSummary;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DashboardStatsSummaryResolver
    implements DataFetcher<CompletableFuture<DashboardStatsSummary>> {

  // The maximum number of top users to show in the summary stats
  private static final Integer MAX_TOP_USERS = 5;

  private final TimeseriesAspectService timeseriesAspectService;

  public DashboardStatsSummaryResolver(final TimeseriesAspectService timeseriesAspectService) {
    this.timeseriesAspectService = timeseriesAspectService;
  }

  @Override
  public CompletableFuture<DashboardStatsSummary> get(DataFetchingEnvironment environment)
      throws Exception {
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            // TODO: We don't have a dashboard specific priv
            if (!isViewDatasetUsageAuthorized(context, resourceUrn)) {
              log.debug(
                  "User {} is not authorized to view usage information for {}",
                  context.getActorUrn(),
                  resourceUrn.toString());
              return null;
            }

            final DashboardStatsSummary result = new DashboardStatsSummary();

            // Obtain total dashboard view count, by viewing the latest reported dashboard metrics.
            List<DashboardUsageMetrics> dashboardUsageMetrics =
                getDashboardUsageMetrics(
                    context, resourceUrn.toString(), null, null, 1, this.timeseriesAspectService);
            if (dashboardUsageMetrics.size() > 0) {
              result.setViewCount(getDashboardViewCount(context, resourceUrn));
            }

            // Obtain unique user statistics, by rolling up unique users over the past month.
            List<DashboardUserUsageCounts> userUsageCounts =
                getDashboardUsagePerUser(context.getOperationContext(), resourceUrn);
            result.setUniqueUserCountLast30Days(userUsageCounts.size());
            result.setTopUsersLast30Days(
                trimUsers(
                    userUsageCounts.stream()
                        .map(DashboardUserUsageCounts::getUser)
                        .collect(Collectors.toList())));

            return result;

          } catch (Exception e) {
            log.error(
                String.format(
                    "Failed to load dashboard usage summary for resource %s",
                    resourceUrn.toString()),
                e);
            return null; // Do not throw when loading usage summary fails.
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private int getDashboardViewCount(@Nullable QueryContext context, final Urn resourceUrn) {
    List<DashboardUsageMetrics> dashboardUsageMetrics =
        getDashboardUsageMetrics(
            context, resourceUrn.toString(), null, null, 1, this.timeseriesAspectService);
    return dashboardUsageMetrics.get(0).getViewsCount();
  }

  private List<DashboardUserUsageCounts> getDashboardUsagePerUser(
      @Nonnull OperationContext opContext, final Urn resourceUrn) {
    long now = System.currentTimeMillis();
    long nowMinusOneMonth = timeMinusOneMonth(now);
    Filter bucketStatsFilter =
        createUsageFilter(resourceUrn.toString(), nowMinusOneMonth, now, true);
    return getUserUsageCounts(opContext, bucketStatsFilter, this.timeseriesAspectService);
  }

  private static List<CorpUser> trimUsers(final List<CorpUser> originalUsers) {
    if (originalUsers.size() > MAX_TOP_USERS) {
      return originalUsers.subList(0, MAX_TOP_USERS);
    }
    return originalUsers;
  }
}
