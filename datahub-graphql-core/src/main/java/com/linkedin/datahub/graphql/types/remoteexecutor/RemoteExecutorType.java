package com.linkedin.datahub.graphql.types.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME;
import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_STATUS_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RemoteExecutor;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;

public class RemoteExecutorType
    implements com.linkedin.datahub.graphql.types.EntityType<RemoteExecutor, String> {
  static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(REMOTE_EXECUTOR_STATUS_ASPECT_NAME);
  private final EntityClient _entityClient;

  public RemoteExecutorType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.REMOTE_EXECUTOR;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<RemoteExecutor> objectClass() {
    return RemoteExecutor.class;
  }

  @Override
  public List<DataFetcherResult<RemoteExecutor>> batchLoadWithoutAuthorization(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> remoteExecutors = urns.stream().map(UrnUtils::getUrn).toList();

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              REMOTE_EXECUTOR_ENTITY_NAME,
              new HashSet<>(remoteExecutors),
              ASPECTS_TO_FETCH);

      return mapResponsesToBatchResults(urns, entities, RemoteExecutorMapper::map, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Remote Executors", e);
    }
  }
}
