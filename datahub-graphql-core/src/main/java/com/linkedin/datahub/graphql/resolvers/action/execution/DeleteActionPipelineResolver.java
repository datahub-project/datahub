package com.linkedin.datahub.graphql.resolvers.action.execution;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class DeleteActionPipelineResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final String actionUrn = environment.getArgument("urn");
    final Urn urn = Urn.createFromString(actionUrn);
    if (AuthorizationUtils.canManageActionPipelines(context)) {
      return _integrationsService
          .stopAction(actionUrn)
          .thenCompose(
              bool ->
                  GraphQLConcurrencyUtils.supplyAsync(
                      () -> {

                        // Delete Action entity from GMS
                        try {
                          _entityClient.deleteEntity(context.getOperationContext(), urn);
                        } catch (Exception e) {
                          throw new RuntimeException(
                              String.format(
                                  "Failed to perform delete against action with urn %s", actionUrn),
                              e);
                        }

                        // Return true if the delete was successful
                        return true;
                      },
                      this.getClass().getSimpleName(),
                      "get"));
    } else {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
  }
}
