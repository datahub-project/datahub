package com.linkedin.datahub.graphql.resolvers.ingest.source;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionResolverUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Gets a particular Ingestion Source by urn. */
@Slf4j
public class GetIngestionSourceResolver implements DataFetcher<CompletableFuture<IngestionSource>> {

  private final EntityClient _entityClient;

  public GetIngestionSourceResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<IngestionSource> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageIngestion(context)) {
      final String urnStr = environment.getArgument("urn");
      return CompletableFuture.supplyAsync(
          () -> {
            try {
              final Urn urn = Urn.createFromString(urnStr);
              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      Constants.INGESTION_SOURCE_ENTITY_NAME,
                      new HashSet<>(ImmutableSet.of(urn)),
                      ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME),
                      context.getAuthentication());
              if (!entities.containsKey(urn)) {
                // No ingestion source found
                throw new DataHubGraphQLException(
                    String.format("Failed to find Ingestion Source with urn %s", urn),
                    DataHubGraphQLErrorCode.NOT_FOUND);
              }
              // Ingestion source found
              return IngestionResolverUtils.mapIngestionSource(entities.get(urn));
            } catch (Exception e) {
              throw new RuntimeException("Failed to retrieve ingestion source", e);
            }
          });
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
