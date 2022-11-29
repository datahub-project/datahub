package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.util.CondUpdateUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.*;


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
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final String condUpdate = environment.getVariables().containsKey(Constants.IN_UNMODIFIED_SINCE)
            ? environment.getVariables().get(Constants.IN_UNMODIFIED_SINCE).toString() : null;
    final QueryContext context = environment.getContext();
    final String testUrn = environment.getArgument("urn");
    final Urn urn = Urn.createFromString(testUrn);
    return CompletableFuture.supplyAsync(() -> {
      if (canManageTests(context)) {
        try {
          Map<String, Long> createdOnMap = CondUpdateUtils.extractCondUpdate(condUpdate);
          _entityClient.deleteEntity(urn, context.getAuthentication(), createdOnMap.get(testUrn));
          return true;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against Test with urn %s", testUrn), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}
