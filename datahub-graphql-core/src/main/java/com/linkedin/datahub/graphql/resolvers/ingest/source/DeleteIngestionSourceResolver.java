package com.linkedin.datahub.graphql.resolvers.ingest.source;

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
 * Resolver responsible for hard deleting a particular DataHub Ingestion Source. Requires MANAGE_INGESTION
 * privilege.
 */
public class DeleteIngestionSourceResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public DeleteIngestionSourceResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final String condUpdate = environment.getVariables().containsKey(Constants.IN_UNMODIFIED_SINCE)
            ? environment.getVariables().get(Constants.IN_UNMODIFIED_SINCE).toString() : null;
    final QueryContext context = environment.getContext();
    if (IngestionAuthUtils.canManageIngestion(context)) {
      final String ingestionSourceUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(ingestionSourceUrn);
      return CompletableFuture.supplyAsync(() -> {
        try {
          Map<String, Long> createdOnMap = CondUpdateUtils.extractCondUpdate(condUpdate);
          _entityClient.deleteEntity(urn, context.getAuthentication(), createdOnMap.get(urn));
          return ingestionSourceUrn;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against ingestion source with urn %s", ingestionSourceUrn), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
