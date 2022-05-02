package com.linkedin.datahub.graphql.types.usage;

import com.datahub.authorization.ResourceSpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.UsageStatsKey;
import com.linkedin.datahub.graphql.VersionedAspectKey;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.usage.UsageClient;
import com.linkedin.usage.UsageQueryResult;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class UsageType implements LoadableType<com.linkedin.datahub.graphql.generated.UsageQueryResult, UsageStatsKey> {
  private final UsageClient _usageClient;

  public UsageType(final UsageClient usageClient) {
    _usageClient = usageClient;
  }

  @Override
  public Class<com.linkedin.datahub.graphql.generated.UsageQueryResult> objectClass() {
    return com.linkedin.datahub.graphql.generated.UsageQueryResult.class;
  }

  @Override
  public String name() {
    return UsageType.class.getSimpleName();
  }

  /**
   * Retrieves an list of aspects given a list of {@link VersionedAspectKey} structs. The list returned is expected to
   * be of same length of the list of keys, where nulls are provided in place of an aspect object if an entity cannot be found.
   * @param keys to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  public List<DataFetcherResult<com.linkedin.datahub.graphql.generated.UsageQueryResult>> batchLoad(
      @Nonnull List<UsageStatsKey> keys, @Nonnull QueryContext context) throws Exception {
    try {
      return keys.stream().map(key -> {
        Urn resourceUrn = UrnUtils.getUrn(key.getResource());
        if (!AuthorizationUtils.isAuthorized(context,
            Optional.of(new ResourceSpec(resourceUrn.getEntityType(), key.getResource())),
            PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE)) {
          log.debug("User {} is not authorized to view usage information for dataset {}", context.getActorUrn(),
              key.getResource());
          return DataFetcherResult.<com.linkedin.datahub.graphql.generated.UsageQueryResult>newResult().build();
        }
        try {
          UsageQueryResult usageQueryResult =
              _usageClient.getUsageStats(key.getResource(), key.getRange(), context.getAuthentication());
          return DataFetcherResult.<com.linkedin.datahub.graphql.generated.UsageQueryResult>newResult().data(
              UsageQueryResultMapper.map(usageQueryResult)).build();
        } catch (RemoteInvocationException | URISyntaxException e) {
          throw new RuntimeException(String.format("Failed to load Usage Stats for resource %s", key.getResource()), e);
        }
      }).collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Usage Stats", e);
    }
  }
}
