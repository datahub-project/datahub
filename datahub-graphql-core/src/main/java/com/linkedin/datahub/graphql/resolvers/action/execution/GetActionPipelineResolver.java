package com.linkedin.datahub.graphql.resolvers.action.execution;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.action.ActionPipelineType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class GetActionPipelineResolver implements DataFetcher<CompletableFuture<ActionPipeline>> {

  private final EntityClient _entityClient;
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<ActionPipeline> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    Optional<String> actionPipelineUrnString = Optional.ofNullable(environment.getArgument("urn"));
    Urn actionPipelineUrn = null;
    if (actionPipelineUrnString.isPresent()) {
      try {
        actionPipelineUrn = Urn.createFromString(actionPipelineUrnString.get());
      } catch (URISyntaxException e) {
        throw new DataHubGraphQLException(
            String.format("Malformed urn %s provided.", actionPipelineUrnString.get()),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    } else {
      throw new DataHubGraphQLException(
          "Action pipeline urn is required.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    if (AuthorizationUtils.canView(context.getOperationContext(), actionPipelineUrn)) {
      Urn finalActionPipelineUrn = actionPipelineUrn;
      // Check if a specific field is requested
      DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();
      CompletableFuture<String> statusFuture = CompletableFuture.completedFuture(null);
      if (selectionSet.contains("status")) {
        statusFuture = _integrationsService.actionStatus(finalActionPipelineUrn.toString());
      }
      return statusFuture.thenCompose(
          status ->
              GraphQLConcurrencyUtils.supplyAsync(
                  () -> {
                    try {
                      // If any field other than status or urn is requested, we need to fetch the
                      // entity
                      // from GMS
                      if (selectionSet.getFields().size() == 2
                          && selectionSet.contains("urn")
                          && selectionSet.contains("status")) {
                        ActionPipeline actionPipeline = new ActionPipeline();
                        actionPipeline.setUrn(finalActionPipelineUrn.toString());
                        actionPipeline.setStatus(status);
                        return actionPipeline;
                      } else {
                        final EntityResponse entityResponse =
                            _entityClient.getV2(
                                context.getOperationContext(),
                                Constants.ACTIONS_PIPELINE_ENTITY_NAME,
                                finalActionPipelineUrn,
                                ActionPipelineType.ASPECTS_TO_FETCH);
                        final ActionPipeline actionPipeline =
                            ActionPipelineType.map(entityResponse);
                        if (status != null && actionPipeline != null) {
                          actionPipeline.setStatus(status);
                        }
                        return actionPipeline;
                      }
                    } catch (Exception e) {
                      log.error("Failed to fetch action pipeline", e);
                      throw new RuntimeException(
                          String.format(
                              "Failed to fetch action pipeline %s",
                              finalActionPipelineUrn.toString()),
                          e);
                    }
                  },
                  this.getClass().getSimpleName(),
                  "get"));
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
