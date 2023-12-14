package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
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

/** Retrieves an Ingestion Execution Request by primary key (urn). */
@Slf4j
public class GetIngestionExecutionRequestResolver
    implements DataFetcher<CompletableFuture<ExecutionRequest>> {

  private final EntityClient _entityClient;

  public GetIngestionExecutionRequestResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ExecutionRequest> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageIngestion(context)) {
      final String urnStr = environment.getArgument("urn");
      return CompletableFuture.supplyAsync(
          () -> {
            try {
              // Fetch specific execution request
              final Urn urn = Urn.createFromString(urnStr);
              final Map<Urn, EntityResponse> entities =
                  _entityClient.batchGetV2(
                      Constants.EXECUTION_REQUEST_ENTITY_NAME,
                      new HashSet<>(ImmutableSet.of(urn)),
                      ImmutableSet.of(
                          Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                          Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME),
                      context.getAuthentication());
              if (!entities.containsKey(urn)) {
                // No execution request found
                throw new DataHubGraphQLException(
                    String.format("Failed to find Execution Request with urn %s", urn),
                    DataHubGraphQLErrorCode.NOT_FOUND);
              }
              // Execution request found
              return IngestionResolverUtils.mapExecutionRequest(entities.get(urn));
            } catch (Exception e) {
              throw new RuntimeException("Failed to retrieve execution request", e);
            }
          });
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
