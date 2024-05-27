package com.linkedin.datahub.graphql.resolvers.action.execution;

import com.linkedin.action.DataHubActionConfig;
import com.linkedin.action.DataHubActionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateActionPipelineInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.key.DataHubActionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


@AllArgsConstructor
@Slf4j
public class StartActionPipelineResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          if (AuthorizationUtils.canManageActionPipelines(context)) {

            final UpdateActionPipelineInput input =
                bindArgument(environment.getArgument("input"), UpdateActionPipelineInput.class);

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
              // When an urn is not provided (e.g. create), we generate an urn from the name.
              final DataHubActionKey key = new DataHubActionKey();
              key.setId(input.getName());
              actionPipelineUrn =
                  EntityKeyUtils.convertEntityKeyToUrn(key, Constants.ACTIONS_PIPELINE_ENTITY_NAME);
            }
            log.info("Action pipeline = {}", actionPipelineUrn);

            try {
              log.info("Action pipeline config = {}", input.getConfig().getRecipe());
              DataHubActionInfo actionInfo = new DataHubActionInfo();
              actionInfo.setType(input.getType());
              actionInfo.setName(input.getName());
              actionInfo.setConfig(
                  new DataHubActionConfig().setRecipe(input.getConfig().getRecipe()));
              log.info("Action Info aspect = {}", actionInfo);

              final MetadataChangeProposal proposal =
                  buildMetadataChangeProposalWithUrn(
                      actionPipelineUrn, Constants.ACTIONS_PIPELINE_INFO_ASPECT_NAME, actionInfo);

              String result =
                  _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

              if (!_integrationsService.reloadAction(result)) {
                throw new DataHubGraphQLException(
                    String.format("Failed to reload action pipeline %s", result),
                    DataHubGraphQLErrorCode.SERVER_ERROR);
              }
              return result;
            } catch (Exception e) {
              log.error("Failed to ingest action pipeline", e);
              throw new RuntimeException(
                  String.format("Failed to create new action pipeline %s", input), e);
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
