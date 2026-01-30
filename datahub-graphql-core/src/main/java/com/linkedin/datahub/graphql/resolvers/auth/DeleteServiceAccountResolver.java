package com.linkedin.datahub.graphql.resolvers.auth;

import static com.datahub.authorization.AuthUtil.isAuthorized;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver for deleting service accounts. */
@Slf4j
public class DeleteServiceAccountResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  public DeleteServiceAccountResolver(final EntityClient entityClient) {
    this._entityClient = entityClient;
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final String urnString = environment.getArgument("urn");

          if (!isAuthorized(
              context.getOperationContext(), PoliciesConfig.MANAGE_SERVICE_ACCOUNTS_PRIVILEGE)) {
            throw new AuthorizationException(
                "Unauthorized to delete service accounts. Please contact your DataHub administrator.");
          }

          try {
            final Urn urn = Urn.createFromString(urnString);

            // Fetch the entity to verify it's a service account
            // Explicitly request SubTypes aspect to check if it's a service account
            final Set<String> aspectsToFetch = new HashSet<>();
            aspectsToFetch.add(Constants.SUB_TYPES_ASPECT_NAME);

            final EntityResponse response =
                _entityClient
                    .batchGetV2(
                        context.getOperationContext(),
                        Constants.CORP_USER_ENTITY_NAME,
                        Collections.singleton(urn),
                        aspectsToFetch)
                    .get(urn);

            if (response == null) {
              throw new IllegalArgumentException("Service account not found: " + urnString);
            }

            log.info(
                "Fetched entity {} with aspects: {}", urnString, response.getAspects().keySet());

            // Verify this is a service account by checking SubTypes aspect
            if (!ServiceAccountUtils.isServiceAccount(response)) {
              log.error(
                  "Entity {} is not a service account. Available aspects: {}",
                  urnString,
                  response.getAspects().keySet());
              throw new IllegalArgumentException(
                  "The specified URN is not a service account: " + urnString);
            }

            log.info("User {} deleting service account {}", context.getActorUrn(), urn.toString());

            // Delete the entity
            _entityClient.deleteEntity(context.getOperationContext(), urn);

            return true;
          } catch (IllegalArgumentException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException("Failed to delete service account", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
