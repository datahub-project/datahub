package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DefaultRemoteExecutorPoolResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RemoteExecutorPool;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class GetDefaultRemoteExecutorPoolResolver
    implements DataFetcher<CompletableFuture<DefaultRemoteExecutorPoolResult>> {

  final EntityClient _entityClient;

  @Override
  public CompletableFuture<DefaultRemoteExecutorPoolResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final DefaultRemoteExecutorPoolResult result = new DefaultRemoteExecutorPoolResult();
            // Get default pool name from global config
            final String defaultPoolName =
                RemoteExecutorUtils.tryGetDefaultExecutorPoolId(
                    _entityClient, context.getOperationContext());
            if (defaultPoolName == null) {
              return result;
            }

            // The type resolver will hydrate this
            final RemoteExecutorPool poolResult = new RemoteExecutorPool();
            poolResult.setType(EntityType.REMOTE_EXECUTOR_POOL);
            poolResult.setUrn(
                new Urn(REMOTE_EXECUTOR_POOL_ENTITY_NAME, defaultPoolName).toString());
            result.setPool(poolResult);
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to get default remote executor pool", e);
          }
        });
  }
}
