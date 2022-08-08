package com.linkedin.datahub.graphql.resolvers.dataset;

import com.datahub.authorization.ResourceSpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.datahub.graphql.types.usage.UsageQueryResultMapper;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.usage.UsageClient;
import com.linkedin.usage.UsageTimeRange;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DatasetUsageStatsResolver implements DataFetcher<CompletableFuture<UsageQueryResult>> {

  private final UsageClient usageClient;

  public DatasetUsageStatsResolver(final UsageClient usageClient) {
    this.usageClient = usageClient;
  }

  @Override
  public CompletableFuture<UsageQueryResult> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final UsageTimeRange range = UsageTimeRange.valueOf(environment.getArgument("range"));

    return CompletableFuture.supplyAsync(() -> {
      if (!isAuthorized(resourceUrn, context)) {
        log.debug("User {} is not authorized to view usage information for dataset {}",
            context.getActorUrn(),
            resourceUrn.toString());
        return null;
      }
      try {
        com.linkedin.usage.UsageQueryResult
            usageQueryResult = usageClient.getUsageStats(resourceUrn.toString(), range, context.getAuthentication());
        return UsageQueryResultMapper.map(usageQueryResult);
      } catch (RemoteInvocationException | URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to load Usage Stats for resource %s", resourceUrn.toString()), e);
      }
    });
  }

  private boolean isAuthorized(final Urn resourceUrn, final QueryContext context) {
    return AuthorizationUtils.isAuthorized(context,
        Optional.of(new ResourceSpec(resourceUrn.getEntityType(), resourceUrn.toString())),
        PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE);
  }
}
