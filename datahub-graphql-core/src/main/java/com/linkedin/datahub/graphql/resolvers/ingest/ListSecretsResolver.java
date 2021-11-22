package com.linkedin.datahub.graphql.resolvers.ingest;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListSecretsInput;
import com.linkedin.datahub.graphql.generated.ListSecretsResult;
import com.linkedin.datahub.graphql.generated.Secret;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.ListResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

public class ListSecretsResolver implements DataFetcher<CompletableFuture<ListSecretsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;

  public ListSecretsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListSecretsResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageSecrets(context)) {
      final ListSecretsInput input = bindArgument(environment.getArgument("input"), ListSecretsInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();

      return CompletableFuture.supplyAsync(() -> {
        try {
          // First, get all secrets
          final ListResult gmsResult = _entityClient.list(Constants.SECRETS_ENTITY_NAME, Collections.emptyMap(), start, count, context.getActor());

          // Then, resolve all secrets
          final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(Constants.SECRETS_ENTITY_NAME, new HashSet<>(gmsResult.getEntities()), context.getActor());

          // Now that we have entities we can bind this to a result.
          final ListSecretsResult result = new ListSecretsResult();
          result.setStart(gmsResult.getStart());
          result.setCount(gmsResult.getCount());
          result.setTotal(gmsResult.getTotal());
          result.setSecrets(mapEntities(entities.values()));
          return result;

        } catch (Exception e) {
          throw new RuntimeException("Failed to list secrets", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<Secret> mapEntities(final Collection<EntityResponse> entities) {
    return null;
  }
}