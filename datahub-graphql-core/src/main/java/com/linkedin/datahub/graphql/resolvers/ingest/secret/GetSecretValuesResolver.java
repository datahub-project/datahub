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

/**
 * Retrieves the plaintext values of secrets stored in DataHub. Uses AES symmetric encryption /
 * decryption. Requires the MANAGE_SECRETS privilege.
 */
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

              // Now for each secret, decrypt and return the value. If no secret was found, then we
              // will simply omit it from the list.
              // There is no ordering guarantee for the list.
              return entities.values().stream()
                  .map(
                      entity -> {
                        EnvelopedAspect aspect =
                            entity.getAspects().get(Constants.SECRET_VALUE_ASPECT_NAME);
                        if (aspect != null) {
                          // Aspect is present.
                          final DataHubSecretValue secretValue =
                              new DataHubSecretValue(aspect.getValue().data());
                          // Now decrypt the encrypted secret.
                          final String decryptedSecretValue = decryptSecret(secretValue.getValue());
                          return new SecretValue(secretValue.getName(), decryptedSecretValue);
                        } else {
                          // No secret exists
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

  private String decryptSecret(final String encryptedSecret) {
    return _secretService.decrypt(encryptedSecret);
  }
}
