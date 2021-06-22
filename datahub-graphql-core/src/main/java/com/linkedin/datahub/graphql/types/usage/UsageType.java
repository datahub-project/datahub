package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.VersionedAspectKey;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.types.aspect.AspectMapper;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.usage.UsageClient;
import graphql.execution.DataFetcherResult;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class UsageType {
  private final UsageClient _usageClient;

  public UsageType(final UsageClient usageClient) {
    _usageClient = usageClient;
  }
  /**
   * Retrieves an list of aspects given a list of {@link VersionedAspectKey} structs. The list returned is expected to
   * be of same length of the list of keys, where nulls are provided in place of an aspect object if an entity cannot be found.
   * @param keys to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  public List<DataFetcherResult<Aspect>> batchLoad(@Nonnull List<VersionedAspectKey> keys, @Nonnull QueryContext context) throws Exception {
    try {
      return keys.stream().map(key -> {
        try {
          VersionedAspect entity = _usageClient.getAspect(key.getUrn(), key.getAspectName(), key.getVersion());
          return DataFetcherResult.<Aspect>newResult().data(AspectMapper.map(entity)).build();
        } catch (RemoteInvocationException e) {
          throw new RuntimeException(String.format("Failed to load Aspect for entity %s", key.getUrn()), e);
        }
      }).collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Aspects", e);
    }
  }
}
