package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.SECRET_VALUE_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateSecretInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.datahub.graphql.types.ingest.secret.mapper.DataHubSecretValueMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates an encrypted DataHub secret. Uses AES symmetric encryption / decryption. Requires the
 * MANAGE_SECRETS privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateSecretResolver implements DataFetcher<CompletableFuture<String>> {
  private final EntityClient entityClient;
  private final SecretService secretService;

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateSecretInput input =
        bindArgument(environment.getArgument("input"), UpdateSecretInput.class);
    final Urn secretUrn = Urn.createFromString(input.getUrn());
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (IngestionAuthUtils.canManageSecrets(context)) {

            try {
              EntityResponse response =
                  entityClient.getV2(
                      context.getOperationContext(),
                      secretUrn.getEntityType(),
                      secretUrn,
                      Set.of(SECRET_VALUE_ASPECT_NAME));
              if (!entityClient.exists(context.getOperationContext(), secretUrn)
                  || response == null) {
                throw new IllegalArgumentException(
                    String.format("Secret for urn %s doesn't exists!", secretUrn));
              }

              DataHubSecretValue updatedVal =
                  DataHubSecretValueMapper.map(
                      response,
                      input.getName(),
                      secretService.encrypt(input.getValue()),
                      input.getDescription(),
                      null);

              final MetadataChangeProposal proposal =
                  buildMetadataChangeProposalWithUrn(
                      secretUrn, SECRET_VALUE_ASPECT_NAME, updatedVal);
              return entityClient.ingestProposal(context.getOperationContext(), proposal, false);
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format(
                      "Failed to update a secret with urn %s and name %s",
                      secretUrn, input.getName()),
                  e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
