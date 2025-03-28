package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME;
import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.UpdateRemoteExecutorPoolInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.executorpool.RemoteExecutorPoolState;
import com.linkedin.executorpool.RemoteExecutorPoolStatus;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateRemoteExecutorPoolResolver implements DataFetcher<CompletableFuture<Boolean>> {
  final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final OperationContext opContext = context.getOperationContext();

    final UpdateRemoteExecutorPoolInput input =
        bindArgument(environment.getArgument("input"), UpdateRemoteExecutorPoolInput.class);
    final Urn poolUrn = Urn.createFromString(input.getUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // 1. Check auth
            if (!AuthorizationUtils.canManageIngestion(context)) {
              throw new Exception(
                  "Insufficient privileges to manage remote executors. Request Manage Ingestion permission.");
            }

            // 2. Get poolInfo
            final RemoteExecutorPoolInfo poolInfo = getPoolInfo(opContext, poolUrn);

            // 3. Update poolInfo
            if (input.getDescription() != null) {
              poolInfo.setDescription(input.getDescription());
            }
            if (input.getReprovision() != null && input.getReprovision()) {
              if (!poolInfo.hasState()) {
                final RemoteExecutorPoolState defaultState = new RemoteExecutorPoolState();
                poolInfo.setState(defaultState);
              }
              final boolean isProvisioningFailed =
                  !poolInfo.getState().hasStatus()
                      || poolInfo
                          .getState()
                          .getStatus()
                          .equals(RemoteExecutorPoolStatus.PROVISIONING_FAILED);
              if (isProvisioningFailed) {
                poolInfo.getState().setStatus(RemoteExecutorPoolStatus.PROVISIONING_PENDING);
              }
            }

            // 4. Ingest updated poolInfo
            _entityClient.ingestProposal(
                opContext,
                AspectUtils.buildMetadataChangeProposal(
                    poolUrn, REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, poolInfo),
                false);
            return true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to update pool", e);
          }
        });
  }

  private RemoteExecutorPoolInfo getPoolInfo(OperationContext opContext, Urn poolUrn)
      throws RemoteInvocationException, URISyntaxException {
    final EntityResponse response =
        _entityClient.getV2(
            opContext,
            REMOTE_EXECUTOR_POOL_ENTITY_NAME,
            poolUrn,
            Set.of(REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME),
            false);
    if (response == null) {
      throw new RuntimeException(String.format("Could not find pool with urn %s", poolUrn));
    }
    final EnvelopedAspect envelopedAspect =
        response.getAspects().get(REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);
    if (envelopedAspect == null) {
      throw new RuntimeException(
          String.format(
              "Could not find aspect %s for pool with urn %s",
              REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, poolUrn));
    }
    return new RemoteExecutorPoolInfo(envelopedAspect.getValue().data());
  }
}
