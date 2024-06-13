package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.*;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for soft-deleting a particular DataHub Test. Requires MANAGE_TESTS
 * privilege.
 *
 * <p>Note that this resolver also removes references to the soft-deleted Test, meaning no assets
 * will have TestResults aspects with this URN inside.
 */
@RequiredArgsConstructor
@Slf4j
public class DeleteTestResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final TestEngine _testEngine;

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

              if (!_entityClient.exists(context.getOperationContext(), urn)) {
                throw new DataHubGraphQLException(
                    String.format("Test with urn %s not found", urn),
                    DataHubGraphQLErrorCode.NOT_FOUND);
              }

              final Status status = new Status();
              status.setRemoved(true);

              _entityClient.ingestProposal(
                  context.getOperationContext(),
                  AspectUtils.buildMetadataChangeProposal(
                      urn, Constants.STATUS_ASPECT_NAME, status),
                  true);

              _testEngine.invalidateCache();

              // Asynchronously Delete all references to the entity (to return quickly)
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      _entityClient.deleteEntityReferences(context.getOperationContext(), urn);
                    } catch (RemoteInvocationException e) {
                      log.error(
                          String.format(
                              "Caught exception while attempting to clear all entity references for Test with urn %s",
                              urn),
                          e);
                    }
                  });
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
