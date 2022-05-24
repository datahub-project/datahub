package com.linkedin.datahub.graphql.types.auth;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.datahub.graphql.types.auth.mappers.AccessTokenMetadataMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AccessTokenMetadataType
    implements com.linkedin.datahub.graphql.types.EntityType<AccessTokenMetadata, String> {

  static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(Constants.ACCESS_TOKEN_INFO_NAME);
  private final EntityClient _entityClient;

  public AccessTokenMetadataType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.ACCESS_TOKEN;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<AccessTokenMetadata> objectClass() {
    return AccessTokenMetadata.class;
  }

  @Override
  public List<DataFetcherResult<AccessTokenMetadata>> batchLoad(@Nonnull List<String> keys,
      @Nonnull QueryContext context) throws Exception {
    final List<Urn> tokenInfoUrns = keys.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(Constants.ACCESS_TOKEN_ENTITY_NAME, new HashSet<>(tokenInfoUrns), ASPECTS_TO_FETCH,
              context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : tokenInfoUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(gmsResult -> gmsResult == null ? null : DataFetcherResult.<AccessTokenMetadata>newResult()
              .data(AccessTokenMetadataMapper.map(gmsResult))
              .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Access Token Info", e);
    }
  }
}
