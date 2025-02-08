package com.linkedin.datahub.graphql.types.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RemoteExecutorPool;
import com.linkedin.datahub.graphql.resolvers.remoteexecutor.RemoteExecutorUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class RemoteExecutorPoolType
    implements com.linkedin.datahub.graphql.types.EntityType<RemoteExecutorPool, String> {
  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  public RemoteExecutorPoolType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.REMOTE_EXECUTOR_POOL;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<RemoteExecutorPool> objectClass() {
    return RemoteExecutorPool.class;
  }

  @Override
  public List<DataFetcherResult<RemoteExecutorPool>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final OperationContext opContext = context.getOperationContext();
    final List<Urn> remoteExecutorPools = urns.stream().map(UrnUtils::getUrn).toList();

    try {
      // 1. get pool aspects
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              opContext,
              REMOTE_EXECUTOR_POOL_ENTITY_NAME,
              new HashSet<>(remoteExecutorPools),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : remoteExecutorPools) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }

      // 2. Get global pool config to determine if one is the default
      final String maybeDefaultPoolId =
          RemoteExecutorUtils.tryGetDefaultExecutorPoolId(_entityClient, opContext);

      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<RemoteExecutorPool>newResult()
                          .data(
                              RemoteExecutorPoolMapper.map(context, gmsResult, maybeDefaultPoolId))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Remote Executor Pools", e);
    }
  }
}
