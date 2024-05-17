package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.UpsertDataHubConnectionInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.connection.ConnectionService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpsertConnectionResolver implements DataFetcher<CompletableFuture<DataHubConnection>> {

  private final ConnectionService _connectionService;
  private final SecretService _secretService;

  public UpsertConnectionResolver(
      @Nonnull final ConnectionService connectionService,
      @Nonnull final SecretService secretService) {
    _connectionService =
        Objects.requireNonNull(connectionService, "connectionService cannot be null");
    _secretService = Objects.requireNonNull(secretService, "secretService cannot be null");
  }

  @Override
  public CompletableFuture<DataHubConnection> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final UpsertDataHubConnectionInput input =
        bindArgument(environment.getArgument("input"), UpsertDataHubConnectionInput.class);
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(
        () -> {
          if (!ConnectionUtils.canManageConnections(context)) {
            throw new AuthorizationException(
                "Unauthorized to upsert Connection. Please contact your DataHub administrator for more information.");
          }

          try {
            final Urn connectionUrn =
                _connectionService.upsertConnection(
                    context.getOperationContext(),
                    input.getId(),
                    UrnUtils.getUrn(input.getPlatformUrn()),
                    DataHubConnectionDetailsType.valueOf(input.getType().toString()),
                    input.getJson() != null
                        // Encrypt payload
                        ? new DataHubJsonConnection()
                            .setEncryptedBlob(_secretService.encrypt(input.getJson().getBlob()))
                        : null,
                    input.getName());

            final EntityResponse connectionResponse =
                _connectionService.getConnectionEntityResponse(
                    context.getOperationContext(), connectionUrn);
            return ConnectionMapper.map(context, connectionResponse, _secretService);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to upsert a Connection from input %s", input), e);
          }
        });
  }
}
