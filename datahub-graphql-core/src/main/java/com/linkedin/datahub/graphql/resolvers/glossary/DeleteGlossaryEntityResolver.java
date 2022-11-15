package com.linkedin.datahub.graphql.resolvers.glossary;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;


@Slf4j
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
    final Urn parentNodeUrn = GlossaryUtils.getParentUrn(entityUrn, context, _entityClient);

    return CompletableFuture.supplyAsync(() -> {
      if (GlossaryUtils.canManageChildrenEntities(context, parentNodeUrn)) {
        if (!_entityService.exists(entityUrn)) {
          throw new RuntimeException(String.format("This urn does not exist: %s", entityUrn));
        }

        try {
          _entityClient.deleteEntity(entityUrn, context.getAuthentication());

          // Asynchronously Delete all references to the entity (to return quickly)
          CompletableFuture.runAsync(() -> {
            try {
              _entityClient.deleteEntityReferences(entityUrn, context.getAuthentication());
            } catch (RemoteInvocationException e) {
              log.error(String.format("Caught exception while attempting to clear all entity references for glossary entity with urn %s", entityUrn), e);
            }
          });

          return true;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against glossary entity with urn %s", entityUrn), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}


