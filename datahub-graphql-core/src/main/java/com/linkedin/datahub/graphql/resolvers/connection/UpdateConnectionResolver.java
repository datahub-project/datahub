package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.UpdateDataHubConnectionInput;
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
public class UpdateConnectionResolver implements DataFetcher<CompletableFuture<DataHubConnection>> {

  private final ConnectionService _connectionService;
  private final SecretService _secretService;
  private final FeatureFlags _featureFlags;

  public UpdateConnectionResolver(
      @Nonnull final ConnectionService connectionService,
      @Nonnull final SecretService secretService,
      @Nonnull final FeatureFlags featureFlags) {
    _connectionService =
        Objects.requireNonNull(connectionService, "connectionService cannot be null");
    _secretService = Objects.requireNonNull(secretService, "secretService cannot be null");
    _featureFlags = Objects.requireNonNull(featureFlags, "featureFlags cannot be null");
  }

  @Override
  public CompletableFuture<DataHubConnection> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateDataHubConnectionInput input =
        bindArgument(environment.getArgument("input"), UpdateDataHubConnectionInput.class);
    final Urn connectionUrn = UrnUtils.getUrn(input.getUrn());
    final Urn platformUrn =
        input.getPlatformUrn() != null ? UrnUtils.getUrn(input.getPlatformUrn()) : null;
    final DataHubConnectionDetailsType type =
        input.getType() != null
            ? DataHubConnectionDetailsType.valueOf(input.getType().toString())
            : null;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!ConnectionUtils.canManageConnections(context)) {
            throw new AuthorizationException(
                "Unauthorized to update Connection. Please contact your DataHub administrator for more information.");
          }

          try {
            final Urn updatedConnectionUrn =
                _connectionService.updateConnection(
                    context.getOperationContext(),
                    connectionUrn,
                    platformUrn,
                    type,
                    input.getJson() != null
                        // Encrypt payload
                        ? new DataHubJsonConnection()
                            .setEncryptedBlob(_secretService.encrypt(input.getJson().getBlob()))
                        : null,
                    input.getName());

            final EntityResponse connectionResponse =
                _connectionService.getConnectionEntityResponse(
                    context.getOperationContext(), updatedConnectionUrn);
            return ConnectionMapper.map(context, connectionResponse, _secretService, _featureFlags);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update a Connection from input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
