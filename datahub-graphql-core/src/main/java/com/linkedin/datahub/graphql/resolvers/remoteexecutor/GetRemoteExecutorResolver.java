package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME;
import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_STATUS_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.RemoteExecutor;
import com.linkedin.datahub.graphql.types.remoteexecutor.RemoteExecutorMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class GetRemoteExecutorResolver implements DataFetcher<CompletableFuture<RemoteExecutor>> {

  final EntityClient _entityClient;

  @Override
  public CompletableFuture<RemoteExecutor> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn urn = UrnUtils.getUrn(environment.getArgument("urn"));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final EntityResponse response =
                _entityClient.getV2(
                    context.getOperationContext(),
                    REMOTE_EXECUTOR_ENTITY_NAME,
                    urn,
                    Set.of(REMOTE_EXECUTOR_STATUS_ASPECT_NAME),
                    false);
            if (response == null) {
              throw new RuntimeException(
                  String.format("Could not find remote executor with urn %s", urn));
            }
            return RemoteExecutorMapper.map(context, response);
          } catch (Exception e) {
            throw new RuntimeException("Failed to get remote executor", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
