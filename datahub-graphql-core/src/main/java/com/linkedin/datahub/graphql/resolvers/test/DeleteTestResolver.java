package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

/**
 * Resolver responsible for hard deleting a particular DataHub Test. Requires MANAGE_TESTS
 * privilege.
 */
public class DeleteTestResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  public DeleteTestResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String testUrn = environment.getArgument("urn");
    final Urn urn = Urn.createFromString(testUrn);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (canManageTests(context)) {
            try {
              _entityClient.deleteEntity(context.getOperationContext(), urn);
              return true;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to perform delete against Test with urn %s", testUrn), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
