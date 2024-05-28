package com.linkedin.datahub.graphql.resolvers.action.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

@AllArgsConstructor
@Slf4j
public class StopActionPipelineResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          if (AuthorizationUtils.canManageActionPipelines(context)) {
            Optional<String> actionPipelineUrnString =
                Optional.ofNullable(environment.getArgument("urn"));
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
                  "Action pipeline urn is required for rollback.",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            }
            log.info("Action pipeline = {}", actionPipelineUrn);

            try {

              if (!_integrationsService.stopAction(actionPipelineUrn.toString())) {
                throw new DataHubGraphQLException(
                    String.format("Failed to rollback action pipeline %s", actionPipelineUrn),
                    DataHubGraphQLErrorCode.SERVER_ERROR);
              }
              return actionPipelineUrn.toString();
            } catch (Exception e) {
              log.error("Failed to rollback action pipeline", e);
              throw new RuntimeException(
                  String.format(
                      "Failed to rollback action pipeline %s", actionPipelineUrn.toString()),
                  e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  private static JSONObject getActionBlock(String type, String recipe) {
    JSONObject actionBlock = new JSONObject();
    actionBlock.put("type", type);
    actionBlock.put("config", new JSONObject(recipe));
    return actionBlock;
  }
}
