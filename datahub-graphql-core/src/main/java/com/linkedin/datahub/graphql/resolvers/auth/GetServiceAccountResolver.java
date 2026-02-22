package com.linkedin.datahub.graphql.resolvers.auth;

import static com.datahub.authorization.AuthUtil.isAuthorized;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ServiceAccount;
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

/** Resolver for getting a single service account by URN. */
@Slf4j
public class GetServiceAccountResolver implements DataFetcher<CompletableFuture<ServiceAccount>> {

  private final EntityClient _entityClient;

  public GetServiceAccountResolver(final EntityClient entityClient) {
    this._entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ServiceAccount> get(DataFetchingEnvironment environment)
      throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final String urnString = environment.getArgument("urn");

          if (!isAuthorized(
              context.getOperationContext(), PoliciesConfig.MANAGE_SERVICE_ACCOUNTS_PRIVILEGE)) {
            throw new AuthorizationException(
                "Unauthorized to view service accounts. Please contact your DataHub administrator.");
          }

          try {
            final Urn urn = Urn.createFromString(urnString);

            // Fetch the entity with all necessary aspects
            // Explicitly request SubTypes and CorpUserInfo aspects
            final Set<String> aspectsToFetch = new HashSet<>();
            aspectsToFetch.add(Constants.SUB_TYPES_ASPECT_NAME);
            aspectsToFetch.add(Constants.CORP_USER_INFO_ASPECT_NAME);
            aspectsToFetch.add(Constants.CORP_USER_KEY_ASPECT_NAME);

            final EntityResponse response =
                _entityClient
                    .batchGetV2(
                        context.getOperationContext(),
                        Constants.CORP_USER_ENTITY_NAME,
                        Collections.singleton(urn),
                        aspectsToFetch)
                    .get(urn);

            if (response == null) {
              return null;
            }

            // Verify this is a service account by checking SubTypes aspect
            if (!ServiceAccountUtils.isServiceAccount(response)) {
              throw new IllegalArgumentException(
                  "The specified URN is not a service account: " + urnString);
            }

            return ServiceAccountUtils.mapToServiceAccount(response);
          } catch (IllegalArgumentException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException("Failed to get service account", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
