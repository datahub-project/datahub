package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;


/**
 * Resolver responsible for hard deleting a particular DataHub Test. Requires MANAGE_TESTS
 * privilege.
 */
public class DeleteTestResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public DeleteTestResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (canManageTests(context)) {
      final String testUrn = environment.getArgument("urn");
      final Urn urn = Urn.createFromString(testUrn);
      return CompletableFuture.supplyAsync(() -> {
        try {
          _entityClient.deleteEntity(urn, context.getAuthentication());
          return testUrn;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against Test with urn %s", testUrn), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  public static boolean canManageTests(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_TESTS_PRIVILEGE);
  }
}
