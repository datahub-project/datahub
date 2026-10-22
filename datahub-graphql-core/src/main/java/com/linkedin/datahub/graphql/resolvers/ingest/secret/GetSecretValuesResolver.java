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

              // Now for each secret, decrypt and return the value. If no secret was found, then we
              // will simply omit it from the list.
              // Secrets that cannot be decrypted are omitted in the same way, so that a single
              // undecryptable secret does not fail the entire batch.
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
                          try {
                            // Now decrypt the encrypted secret.
                            final String decryptedSecretValue =
                                _secretService.decrypt(
                                    context.getOperationContext(), secretValue.getValue());
                            return new SecretValue(secretValue.getName(), decryptedSecretValue);
                          } catch (SecurityException e) {
                            // The service's caller guard denied the actor before any cipher work.
                            // That is an authorization failure of the whole request, not a
                            // property of this secret's stored value, so it must keep failing
                            // the request instead of being mistaken for a bad ciphertext.
                            throw e;
                          } catch (Exception e) {
                            // Isolate the failure to this secret so that the healthy secrets in
                            // the batch still resolve. Never log the secret value itself.
                            log.warn(
                                "Failed to decrypt secret {}. Its stored value is undecryptable, "
                                    + "likely encrypted with a different encryption key. Omitting "
                                    + "it from the response.",
                                secretValue.getName(),
                                e);
                            return null;
                          }
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
}
