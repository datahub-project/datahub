package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetSecretValuesInput;
import com.linkedin.datahub.graphql.generated.SecretValue;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Retrieves the plaintext values of secrets stored in DataHub. Uses AES symmetric encryption /
 * decryption. Requires the MANAGE_SECRETS privilege.
 */
@Slf4j
public class GetSecretValuesResolver implements DataFetcher<CompletableFuture<List<SecretValue>>> {

  private final EntityClient _entityClient;
  private final SecretService _secretService;

  public GetSecretValuesResolver(
      final EntityClient entityClient, final SecretService secretService) {
    _entityClient = entityClient;
    _secretService = secretService;
  }

  @Override
  public CompletableFuture<List<SecretValue>> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageSecrets(context)) {

      final GetSecretValuesInput input =
          bindArgument(environment.getArgument("input"), GetSecretValuesInput.class);

      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              // Fetch secrets
              final Set<Urn> urns =
                  input.getSecrets().stream()
                      .map(urnStr -> Urn.createFromTuple(Constants.SECRETS_ENTITY_NAME, urnStr))
                      .collect(Collectors.toSet());

              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      Constants.SECRETS_ENTITY_NAME,
                      new HashSet<>(urns),
                      ImmutableSet.of(Constants.SECRET_VALUE_ASPECT_NAME));

              // Now for each secret, decrypt and return the value. If no secret was found, or its
              // stored value cannot be decrypted, it is omitted from the list so that the
              // remaining secrets in the batch still resolve.
              // There is no ordering guarantee for the list.
              return entities.values().stream()
                  .map(
                      entity -> {
                        EnvelopedAspect aspect =
                            entity.getAspects().get(Constants.SECRET_VALUE_ASPECT_NAME);
                        if (aspect == null) {
                          // No secret exists
                          return null;
                        }
                        final DataHubSecretValue secretValue =
                            new DataHubSecretValue(aspect.getValue().data());
                        try {
                          // Now decrypt the encrypted secret.
                          final String decryptedSecretValue =
                              _secretService.decrypt(
                                  context.getOperationContext(), secretValue.getValue());
                          return new SecretValue(secretValue.getName(), decryptedSecretValue);
                        } catch (Exception e) {
                          // A stored value encrypted with a different SECRET_SERVICE_ENCRYPTION_KEY
                          // (e.g. after a migration) or with corrupted encoding is undecryptable;
                          // failing the whole batch for it would also break every healthy secret
                          // requested alongside it.
                          log.error(
                              "Failed to decrypt secret '{}' (urn: {}). The stored value is likely "
                                  + "corrupted or was encrypted with a different encryption key; "
                                  + "delete and re-create the secret to fix it. Omitting it from "
                                  + "the response. Reason: {}",
                              secretValue.getName(),
                              entity.getUrn(),
                              e.getMessage());
                          return null;
                        }
                      })
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to perform update against input %s", input.toString()), e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
