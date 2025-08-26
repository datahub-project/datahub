package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DeleteDataHubConnectionInput;
import com.linkedin.metadata.connection.ConnectionService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteConnectionResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final com.linkedin.metadata.connection.ConnectionService _connectionService;

  public DeleteConnectionResolver(@Nonnull final ConnectionService connectionService) {
    _connectionService =
        Objects.requireNonNull(connectionService, "connectionService cannot be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final DeleteDataHubConnectionInput input =
        bindArgument(environment.getArgument("input"), DeleteDataHubConnectionInput.class);
    final Urn connectionUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!ConnectionUtils.canManageConnections(context)) {
            throw new AuthorizationException(
                "Unauthorized to delete Connection. Please contact your DataHub administrator for more information.");
          }

          try {
            if (input.getHardDelete() != null && input.getHardDelete()) {
              _connectionService.deleteConnection(context.getOperationContext(), connectionUrn);
            } else {
              _connectionService.softDeleteConnection(context.getOperationContext(), connectionUrn);
            }
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete a Connection from input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
