package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.datahub.graphql.VersionedAspectKey;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.RestLiResponseException;
import graphql.execution.DataFetcherResult;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AspectType {
  private final AspectClient _aspectClient;

  public AspectType(final AspectClient aspectClient) {
    _aspectClient = aspectClient;
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
          VersionedAspect entity = _aspectClient.getAspect(key.getUrn(), key.getAspectName(), key.getVersion());
          return DataFetcherResult.<Aspect>newResult().data(AspectMapper.map(entity)).build();
        } catch (RemoteInvocationException e) {
          if (e instanceof RestLiResponseException) {
            // if no aspect is found, restli will return a 404 rather than null
            // https://linkedin.github.io/rest.li/user_guide/restli_server#returning-nulls
            if (((RestLiResponseException) e).getStatus() == 404) {
              return DataFetcherResult.<Aspect>newResult().data(null).build();
            }
          }
          throw new RuntimeException(String.format("Failed to load Aspect for entity %s", key.getUrn()), e);
        }
      }).collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Aspects", e);
    }
  }
}
