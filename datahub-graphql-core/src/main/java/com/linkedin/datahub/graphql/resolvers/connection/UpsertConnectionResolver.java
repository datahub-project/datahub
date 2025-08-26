package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.UpsertDataHubConnectionInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpsertConnectionResolver implements DataFetcher<CompletableFuture<DataHubConnection>> {

  // TODO: Replace with constant reference once PR #3517 is in.
  private static final Urn SLACK_CONNECTION_URN =
      UrnUtils.getUrn("urn:li:dataHubConnection:__system_slack-0");
  private static final Set<Urn> INTEGRATIONS_SERVICES_CONNECTIONS =
      ImmutableSet.of(SLACK_CONNECTION_URN);
  private final ConnectionService _connectionService;
  private final SecretService _secretService;
  private final FeatureFlags _featureFlags;
  private final IntegrationsService _integrationsService;

  public UpsertConnectionResolver(
      @Nonnull final ConnectionService connectionService,
      @Nonnull final SecretService secretService,
      @Nonnull final IntegrationsService integrationsService,
      @Nonnull final FeatureFlags featureFlags) {
    _connectionService =
        Objects.requireNonNull(connectionService, "connectionService cannot be null");
    _secretService = Objects.requireNonNull(secretService, "secretService cannot be null");
    _integrationsService =
        Objects.requireNonNull(integrationsService, "integrationsService cannot be null");
    _featureFlags = Objects.requireNonNull(featureFlags, "featureFlags cannot be null");
  }

  @Override
  public CompletableFuture<DataHubConnection> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final UpsertDataHubConnectionInput input =
        bindArgument(environment.getArgument("input"), UpsertDataHubConnectionInput.class);
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
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

            performPostUpsertActions(connectionUrn);

            return ConnectionMapper.map(context, connectionResponse, _secretService, _featureFlags);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to upsert a Connection from input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Acryl-Only: Optionally call Integrations service to refresh the credentials after upserting a
   * connection.
   */
  private void performPostUpsertActions(@Nonnull final Urn connectionUrn) {
    /* Reload credentials in integrations service after update. */
    if (INTEGRATIONS_SERVICES_CONNECTIONS.contains(connectionUrn)) {
      _integrationsService.reloadCredentials();
    }
  }
}
