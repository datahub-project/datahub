package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.datahub.graphql.util.CondUpdateUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Hard deletes a particular DataHub secret. Requires the MANAGE_SECRETS privilege.
 */
public class DeleteSecretResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public DeleteSecretResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final String condUpdate = environment.getVariables().containsKey(Constants.IN_UNMODIFIED_SINCE)
            ? environment.getVariables().get(Constants.IN_UNMODIFIED_SINCE).toString() : null;
    final QueryContext context = environment.getContext();
    if (IngestionAuthUtils.canManageSecrets(context)) {
      final String secretUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(secretUrn);
      return CompletableFuture.supplyAsync(() -> {
        try {
          Map<String, Long> createdOnMap = CondUpdateUtils.extractCondUpdate(condUpdate);
          _entityClient.deleteEntity(urn, context.getAuthentication(), createdOnMap.get(urn.toString()));
          return secretUrn;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against secret with urn %s", secretUrn), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
