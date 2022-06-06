package com.linkedin.datahub.graphql.resolvers.glossary;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;


public class DeleteGlossaryEntityResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  public DeleteGlossaryEntityResolver(final EntityClient entityClient, EntityService entityService) {
    _entityClient = entityClient;
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn entityUrn = Urn.createFromString(environment.getArgument("urn"));

    return CompletableFuture.supplyAsync(() -> {
      if (AuthorizationUtils.canManageGlossaries(environment.getContext())) {
        if (!_entityService.exists(entityUrn)) {
          throw new RuntimeException(String.format("This urn does not exist: %s", entityUrn));
        }

        try {
          _entityClient.deleteEntity(entityUrn, context.getAuthentication());
          return true;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against glossary entity with urn %s", entityUrn), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}


