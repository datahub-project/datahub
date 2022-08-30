package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RollbackIngestionInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

public class RollbackIngestionResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityClient _entityClient;

  public RollbackIngestionResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (!IngestionAuthUtils.canManageIngestion(context)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      final RollbackIngestionInput input = bindArgument(environment.getArgument("input"), RollbackIngestionInput.class);
      final String runId = input.getRunId();

      rollbackIngestion(runId, context);
      return true;
    });
  }

  public CompletableFuture<Boolean> rollbackIngestion(final String runId, final QueryContext context) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        _entityClient.rollbackIngestion(runId, context.getAuthentication());
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Failed to rollback ingestion execution", e);
      }
    });

  }

}
