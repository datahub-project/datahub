package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateSecretInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.datahub.graphql.types.ingest.secret.mapper.DataHubSecretValueMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DataHubSecretKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.concurrent.CompletableFuture;

/**
 * Creates an encrypted DataHub secret. Uses AES symmetric encryption / decryption. Requires the
 * MANAGE_SECRETS privilege.
 */
public class CreateSecretResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final SecretService _secretService;

  public CreateSecretResolver(final EntityClient entityClient, final SecretService secretService) {
    _entityClient = entityClient;
    _secretService = secretService;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateSecretInput input =
        bindArgument(environment.getArgument("input"), CreateSecretInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (IngestionAuthUtils.canManageSecrets(context)) {

            try {
              // Create the Ingestion source key --> use the display name as a unique id to ensure
              // it's not duplicated.
              final DataHubSecretKey key = new DataHubSecretKey();
              key.setId(input.getName());

              if (_entityClient.exists(
                  context.getOperationContext(),
                  EntityKeyUtils.convertEntityKeyToUrn(key, SECRETS_ENTITY_NAME))) {
                throw new IllegalArgumentException("This Secret already exists!");
              }

              // Create the secret value.
              final DataHubSecretValue value =
                  DataHubSecretValueMapper.map(
                      null,
                      input.getName(),
                      _secretService.encrypt(input.getValue()),
                      input.getDescription(),
                      new AuditStamp()
                          .setActor(UrnUtils.getUrn(context.getActorUrn()))
                          .setTime(System.currentTimeMillis()));

              final MetadataChangeProposal proposal =
                  buildMetadataChangeProposalWithKey(
                      key, SECRETS_ENTITY_NAME, SECRET_VALUE_ASPECT_NAME, value);
              return _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to create new secret with name %s", input.getName()), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
