package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

/** Hard deletes a particular DataHub secret. Requires the MANAGE_SECRETS privilege. */
public class DeleteSecretResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public DeleteSecretResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (IngestionAuthUtils.canManageSecrets(context)) {
      final String inputUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(inputUrn);
      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              _entityClient.deleteEntity(context.getOperationContext(), urn);
              return inputUrn;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to perform delete against secret with urn %s", inputUrn),
                  e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
