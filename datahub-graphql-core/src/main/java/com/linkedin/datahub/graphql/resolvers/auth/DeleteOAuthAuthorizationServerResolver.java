package com.linkedin.datahub.graphql.resolvers.auth;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.connection.ConnectionUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Deletes an OAuth Authorization Server entity. */
@Slf4j
public class DeleteOAuthAuthorizationServerResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient entityClient;

  public DeleteOAuthAuthorizationServerResolver(@Nonnull final EntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final Urn serverUrn = UrnUtils.getUrn(urnStr);

    // Reuse MANAGE_CONNECTIONS privilege since OAuth auth servers are external connections
    if (!ConnectionUtils.canManageConnections(context)) {
      throw new AuthorizationException(
          "Unauthorized to delete OAuth authorization servers. "
              + "Please contact your DataHub administrator.");
    }

    // Verify it's an OAuth authorization server URN
    if (!Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME.equals(serverUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("URN %s is not an OAuth authorization server URN", serverUrn));
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            entityClient.deleteEntity(context.getOperationContext(), serverUrn);
            log.info("Deleted OAuth authorization server: {}", serverUrn);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to delete OAuth authorization server %s", serverUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
