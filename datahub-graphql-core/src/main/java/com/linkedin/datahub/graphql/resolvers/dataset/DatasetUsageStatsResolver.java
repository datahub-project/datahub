package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.isViewDatasetUsageAuthorized;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.datahub.graphql.types.usage.UsageQueryResultMapper;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.usage.UsageClient;
import com.linkedin.usage.UsageTimeRange;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasetUsageStatsResolver implements DataFetcher<CompletableFuture<UsageQueryResult>> {

  private final UsageClient usageClient;

  public DatasetUsageStatsResolver(final UsageClient usageClient) {
    this.usageClient = usageClient;
  }

  @Override
  public CompletableFuture<UsageQueryResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final UsageTimeRange range = UsageTimeRange.valueOf(environment.getArgument("range"));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!isViewDatasetUsageAuthorized(context, resourceUrn)) {
            log.debug(
                "User {} is not authorized to view usage information for dataset {}",
                context.getActorUrn(),
                resourceUrn.toString());
            return null;
          }
          try {
            com.linkedin.usage.UsageQueryResult usageQueryResult =
                usageClient.getUsageStats(
                    context.getOperationContext(), resourceUrn.toString(), range);
            return UsageQueryResultMapper.map(context, usageQueryResult);
          } catch (Exception e) {
            log.error(String.format("Failed to load Usage Stats for resource %s", resourceUrn), e);
            MetricUtils.counter(this.getClass(), "usage_stats_dropped").inc();
          }

          return UsageQueryResultMapper.EMPTY;
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
