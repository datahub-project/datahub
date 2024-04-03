package com.linkedin.datahub.graphql.resolvers.ingest.source;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

/**
 * Resolver responsible for hard deleting a particular DataHub Ingestion Source. Requires
 * MANAGE_INGESTION privilege.
 */
public class DeleteIngestionSourceResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public DeleteIngestionSourceResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (IngestionAuthUtils.canManageIngestion(context)) {
      final String ingestionSourceUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(ingestionSourceUrn);
      return CompletableFuture.supplyAsync(
          () -> {
            try {
              _entityClient.deleteEntity(urn, context.getAuthentication());
              return ingestionSourceUrn;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format(
                      "Failed to perform delete against ingestion source with urn %s",
                      ingestionSourceUrn),
                  e);
            }
          });
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
