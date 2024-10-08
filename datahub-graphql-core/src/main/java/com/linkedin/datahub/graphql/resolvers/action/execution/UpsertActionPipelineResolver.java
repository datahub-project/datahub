package com.linkedin.datahub.graphql.resolvers.action.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.action.DataHubActionConfig;
import com.linkedin.action.DataHubActionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateActionPipelineInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.key.DataHubActionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class UpsertActionPipelineResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final IntegrationsService _integrationsService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    if (AuthorizationUtils.canManageActionPipelines(context)) {

      final UpdateActionPipelineInput input =
          bindArgument(environment.getArgument("input"), UpdateActionPipelineInput.class);

      Optional<String> actionPipelineUrnString =
          Optional.ofNullable(environment.getArgument("urn"));
      Urn actionPipelineUrn;
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

        // Generate UUID for URN
        final String id = UUID.randomUUID().toString();
        key.setId(id);

        // Set the URN
        actionPipelineUrn =
            EntityKeyUtils.convertEntityKeyToUrn(key, Constants.ACTIONS_PIPELINE_ENTITY_NAME);
      }
      log.info("Action pipeline = {}", actionPipelineUrn);
      CompletableFuture<String> ingestProposal =
          GraphQLConcurrencyUtils.supplyAsync(
              () -> {
                log.info("Action pipeline config = {}", input.getConfig().getRecipe());
                DataHubActionInfo actionInfo = new DataHubActionInfo();
                actionInfo.setType(input.getType());
                actionInfo.setName(input.getName());
                actionInfo.setCategory(input.getCategory(), SetMode.IGNORE_NULL);
                actionInfo.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
                actionInfo.setConfig(
                    new DataHubActionConfig()
                        .setRecipe(input.getConfig().getRecipe())
                        .setExecutorId(input.getConfig().getExecutorId()));
                log.info("Action Info aspect = {}", actionInfo);

                MetadataChangeProposal proposal =
                    AspectUtils.buildSynchronousMetadataChangeProposal(
                        actionPipelineUrn, Constants.ACTIONS_PIPELINE_INFO_ASPECT_NAME, actionInfo);
                try {
                  return _entityClient.ingestProposal(
                      context.getOperationContext(), proposal, false);
                } catch (RemoteInvocationException e) {
                  throw new RuntimeException(e);
                }
              },
              this.getClass().getSimpleName(),
              "ingestProposal");

      return ingestProposal.thenCompose(
          result ->
              _integrationsService
                  .reloadAction(result)
                  .thenCompose(
                      reloaded ->
                          GraphQLConcurrencyUtils.supplyAsync(
                              () -> {
                                try {
                                  if (!reloaded) {
                                    throw new DataHubGraphQLException(
                                        String.format(
                                            "Failed to reload action pipeline %s", result),
                                        DataHubGraphQLErrorCode.SERVER_ERROR);
                                  }

                                  return result;
                                } catch (Exception e) {
                                  log.error("Failed to ingest action pipeline", e);
                                  throw new RuntimeException(
                                      String.format(
                                          "Failed to create new action pipeline %s", input),
                                      e);
                                }
                              },
                              this.getClass().getSimpleName(),
                              "get")));
    } else {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
  }
}
