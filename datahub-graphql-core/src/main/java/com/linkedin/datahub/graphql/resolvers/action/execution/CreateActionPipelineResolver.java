package com.linkedin.datahub.graphql.resolvers.action.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.linkedin.action.DataHubActionConfig;
import com.linkedin.action.DataHubActionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateActionPipelineInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DataHubActionKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

@AllArgsConstructor
@Slf4j
public class CreateActionPipelineResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  private static JSONObject getSourceBlock() {
    return new JSONObject(
        "{\n"
            + "            \"type\": \"kafka\",\n"
            + "            \"config\": {\n"
            + "                \"connection\": {\n"
            + "                    \"bootstrap\": \"broker:29092\",\n"
            + "                    \"schema_registry_url\": \"http://schema-registry:8081\",\n"
            + "                }\n"
            + "            }\n"
            + "        }");
  }

  private static JSONObject getServerBlock() {
    return new JSONObject(
        "{\n" + "            \"server\": \"http://datahub-gms:8080\"\n" + "        }");
  }

  private static JSONObject getFilterBlock() {
    return new JSONObject(
        "{\n" + "            \"event_type\": \"EntityChangeEvent_v1\"\n" + "        }");
  }

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          if (IngestionAuthUtils.canManageIngestion(context)) {

            final UpdateActionPipelineInput input =
                bindArgument(environment.getArgument("input"), UpdateActionPipelineInput.class);

            try {
              JSONObject actionPipeline = new JSONObject();
              actionPipeline.put("name", input.getName());
              actionPipeline.put("source", getSourceBlock());
              actionPipeline.put("filter", getFilterBlock());
              actionPipeline.put("datahub", getServerBlock());
              actionPipeline.put(
                  "action", getActionBlock(input.getType(), input.getConfig().getRecipe()));

              // Fetch the original ingestion source
              final DataHubActionKey key = new DataHubActionKey();
              key.setId(input.getName());
              final Urn actionPipelineUrn =
                  EntityKeyUtils.convertEntityKeyToUrn(key, "dataHubAction");
              log.info("Action pipeline = {}", actionPipelineUrn);
              log.info("Action pipeline config = {}", input.getConfig().getRecipe());
              DataHubActionInfo actionInfo = new DataHubActionInfo();
              actionInfo.setType(input.getType());
              actionInfo.setName(input.getName());
              actionInfo.setConfig(new DataHubActionConfig().setRecipe(actionPipeline.toString()));
              log.info("Action Info aspect = {}", actionInfo);

              final MetadataChangeProposal proposal =
                  buildMetadataChangeProposalWithUrn(
                      actionPipelineUrn, "dataHubActionInfo", actionInfo);
              log.info("Entity client = {}", _entityClient);
              log.info("proposal = {}", proposal);
              String result =
                  _entityClient.ingestProposal(proposal, context.getAuthentication(), false);
              log.info("result = {}", result);
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
