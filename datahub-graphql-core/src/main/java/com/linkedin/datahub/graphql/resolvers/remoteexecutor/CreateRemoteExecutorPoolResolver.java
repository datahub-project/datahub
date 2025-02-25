package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME;
import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.CreateRemoteExecutorPoolInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.metadata.entity.AspectUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CreateRemoteExecutorPoolResolver implements DataFetcher<CompletableFuture<String>> {
  final EntityClient _entityClient;
  final LongSupplier _timeProvider;

  public CreateRemoteExecutorPoolResolver(EntityClient entityClient) {
    this(entityClient, System::currentTimeMillis);
  }

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final OperationContext opContext = context.getOperationContext();

    final CreateRemoteExecutorPoolInput input =
        bindArgument(environment.getArgument("input"), CreateRemoteExecutorPoolInput.class);
    final Urn poolUrn =
        Urn.createFromTuple(REMOTE_EXECUTOR_POOL_ENTITY_NAME, input.getExecutorPoolId());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // 1. Check auth
            if (!AuthorizationUtils.canManageIngestion(context)) {
              throw new Exception("Insufficient privileges to manage remote executors.");
            }

            // 2. Create a pool
            final RemoteExecutorPoolInfo poolInfo = new RemoteExecutorPoolInfo();
            poolInfo.setCreatedAt(_timeProvider.getAsLong());
            poolInfo.setCreator(UrnUtils.getUrn(context.getActorUrn()));
            if (input.getDescription() != null) {
              poolInfo.setDescription(input.getDescription());
            }
            _entityClient.ingestProposal(
                opContext,
                AspectUtils.buildMetadataChangeProposal(
                    poolUrn, REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, poolInfo),
                false);

            // 3. Set default if set on params
            if (input.getIsDefault()) {
              RemoteExecutorUtils.updateDefaultRemoteExecutorPool(
                  _entityClient, opContext, poolUrn.getId());
            }

            return poolUrn.toString();
          } catch (Exception e) {
            throw new RuntimeException("Failed to create a new pool", e);
          }
        });
  }
}
