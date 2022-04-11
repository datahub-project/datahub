package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.VersionedAspectKey;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.util.Pair;
import graphql.execution.DataFetcherResult;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AspectType {
  private final EntityClient _entityClient;

  public AspectType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  /**
   * Retrieves an list of aspects given a list of {@link VersionedAspectKey} structs. The list returned is expected to
   * be of same length of the list of keys, where nulls are provided in place of an aspect object if an entity cannot be found.
   *
   * @param keys to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  public List<DataFetcherResult<Aspect>> batchLoad(@Nonnull List<VersionedAspectKey> keys, @Nonnull QueryContext context) throws Exception {

    try {
      return keys.stream().map(key -> {
        try {
          Urn entityUrn = Urn.createFromString(key.getUrn());

          EntityResponse response = _entityClient.getVersionedEntity(
              entityUrn.getEntityType(),
              entityUrn,
              key.getAspectName(),
              key.getVersion(),
              context.getAuthentication()
          );

          if (response == null || response.getAspects().get(key.getAspectName()) == null) {
            // The aspect was not found. Return null.
            return DataFetcherResult.<Aspect>newResult().data(null).build();
          }
          Pair<VersionedAspectKey, EntityResponse> responsePair = new Pair<>(key, response);
          return DataFetcherResult.<Aspect>newResult().data(AspectMapper.map(responsePair)).build();
        } catch (Exception e) {
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
