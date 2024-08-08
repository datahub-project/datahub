package com.linkedin.datahub.graphql.resolvers.group;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for hard deleting a particular DataHub Corp Group */
@Slf4j
public class RemoveGroupResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  public RemoveGroupResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final String groupUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(groupUrn);
      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              _entityClient.deleteEntity(context.getOperationContext(), urn);

              // Asynchronously Delete all references to the entity (to return quickly)
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      _entityClient.deleteEntityReferences(context.getOperationContext(), urn);
                    } catch (Exception e) {
                      log.error(
                          String.format(
                              "Caught exception while attempting to clear all entity references for group with urn %s",
                              urn),
                          e);
                    }
                  });

              return true;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to perform delete against group with urn %s", groupUrn), e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
