package com.linkedin.datahub.graphql.types.connection;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.connection.ConnectionMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.SecretService;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DataHubConnectionType
    implements com.linkedin.datahub.graphql.types.EntityType<DataHubConnection, String> {

  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
          Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
  private final EntityClient _entityClient;
  private final SecretService _secretService;

  public DataHubConnectionType(
      @Nonnull final EntityClient entityClient, @Nonnull final SecretService secretService) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _secretService = Objects.requireNonNull(secretService, "secretService must not be null");
  }

  @Override
  public EntityType type() {
    return EntityType.DATAHUB_CONNECTION;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<DataHubConnection> objectClass() {
    return DataHubConnection.class;
  }

  @Override
  public List<DataFetcherResult<DataHubConnection>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> connectionUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.DATAHUB_CONNECTION_ENTITY_NAME,
              new HashSet<>(connectionUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : connectionUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<DataHubConnection>newResult()
                          .data(ConnectionMapper.map(context, gmsResult, _secretService))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Connections", e);
    }
  }
}
