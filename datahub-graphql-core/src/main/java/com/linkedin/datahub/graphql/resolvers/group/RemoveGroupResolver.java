package com.linkedin.datahub.graphql.resolvers.group;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.RestliEntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

/**
 * Resolver responsible for hard deleting a particular DataHub Corp User
 */
public class RemoveGroupResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final RestliEntityClient _entityClient;

  public RemoveGroupResolver(final RestliEntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final String groupUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(groupUrn);
      return CompletableFuture.supplyAsync(() -> {
        try {
          _entityClient.deleteEntity(urn, context.getActor());
          return true;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against group with urn %s", groupUrn), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
