package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.AspectLoadKey;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.generated.DownstreamEntityRelationships;
import com.linkedin.datahub.graphql.types.relationships.mappers.DownstreamEntityRelationshipsMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.lineage.client.Lineages;
import com.linkedin.metadata.aspect.AspectWithMetadata;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AspectType {
  private final AspectClient _aspectClient;

  public AspectType(final AspectClient aspectClient) {
    _aspectClient = aspectClient;
  }
  /**
   * Retrieves an list of entities given a list of urn strings. The list returned is expected to
   * be of same length of the list of urns, where nulls are provided in place of an entity object if an entity cannot be found.
   * @param keys to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  public List<DataFetcherResult<Aspect>> batchLoad(@Nonnull List<AspectLoadKey> keys, @Nonnull QueryContext context) throws Exception {
    try {
      return keys.stream().map(key -> {
        try {
          AspectWithMetadata entity = _aspectClient.getAspect(key.getUrn(), key.getAspectName(), key.getVersion());
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
