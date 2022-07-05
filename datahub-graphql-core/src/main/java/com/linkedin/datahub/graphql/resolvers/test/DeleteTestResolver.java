package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.*;


/**
 * Resolver responsible for hard deleting a particular DataHub Test. Requires MANAGE_TESTS
 * privilege.
 */
@RequiredArgsConstructor
@Slf4j
public class DeleteTestResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final TestEngine _testEngine;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String testUrn = environment.getArgument("urn");
    final Urn urn = Urn.createFromString(testUrn);
    return CompletableFuture.supplyAsync(() -> {
      if (canManageTests(context)) {
        try {
          _entityClient.deleteEntity(urn, context.getAuthentication());
          _testEngine.invalidateCache();

          // Asynchronously Delete all references to the entity (to return quickly)
          CompletableFuture.runAsync(() -> {
            try {
              _entityClient.deleteEntityReferences(urn, context.getAuthentication());
            } catch (RemoteInvocationException e) {
              log.error(String.format(
                  "Caught exception while attempting to clear all entity references for Test with urn %s", urn), e);
            }
          });
          return true;
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform delete against Test with urn %s", testUrn), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}
