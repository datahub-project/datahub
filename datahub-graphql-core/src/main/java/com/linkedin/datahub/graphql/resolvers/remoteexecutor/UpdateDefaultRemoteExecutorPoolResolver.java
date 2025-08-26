package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class UpdateDefaultRemoteExecutorPoolResolver
    implements DataFetcher<CompletableFuture<Boolean>> {
  final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final OperationContext opContext = context.getOperationContext();

    final String urnStr = environment.getArgument("urn");
    final Urn urn = UrnUtils.getUrn(urnStr);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // 1. Ensure user has permissions
            if (!AuthorizationUtils.canManageIngestion(context)) {
              throw new Exception("Insufficient privileges to manage remote executors.");
            }
            // 2. Ensure pool exists
            final EntityResponse response =
                _entityClient.getV2(
                    opContext,
                    REMOTE_EXECUTOR_POOL_ENTITY_NAME,
                    urn,
                    Set.of(REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME));
            if (response == null || !response.hasAspects()) {
              throw new Exception("Pool not found");
            }

            // 3. update default
            RemoteExecutorUtils.updateDefaultRemoteExecutorPool(
                _entityClient, opContext, urn.getId());

            return true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to update the default pool", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
