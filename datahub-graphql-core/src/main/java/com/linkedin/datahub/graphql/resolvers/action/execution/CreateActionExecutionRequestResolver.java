package com.linkedin.datahub.graphql.resolvers.action.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.action.DataHubActionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateActionExecutionRequestInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.metadata.config.ActionPipelineConfiguration;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.json.JSONException;
import org.json.JSONObject;

/** Creates an on-demand action pipeline request. */
public class CreateActionExecutionRequestResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private static final String RUN_INGEST_TASK_NAME = "RUN_INGEST";
  private static final String MANUAL_EXECUTION_SOURCE_NAME = "MANUAL_INGESTION_SOURCE";
  private static final String RECIPE_ARG_NAME = "recipe";
  private static final String VERSION_ARG_NAME = "version";
  private static final String DEBUG_MODE_ARG_NAME = "debug_mode";
  private static final String ACTION_PIPELINE_ENTITY_NAME = "datahubAction";
  private static final String ACTION_INFO_ASPECT_NAME = "dataHubActionInfo";

  private final EntityClient _entityClient;

  private final IntegrationsService _integrationsService;
  private final ActionPipelineConfiguration _actionPipelineConfiguration;

  public CreateActionExecutionRequestResolver(
      final EntityClient entityClient,
      final IntegrationsService integrationsService,
      final ActionPipelineConfiguration actionConfiguration) {
    _entityClient = entityClient;
    _integrationsService = integrationsService;
    _actionPipelineConfiguration = actionConfiguration;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          if (IngestionAuthUtils.canManageIngestion(context)) {

            final CreateActionExecutionRequestInput input =
                bindArgument(
                    environment.getArgument("input"), CreateActionExecutionRequestInput.class);

            try {
              final ExecutionRequestKey key = new ExecutionRequestKey();
              final UUID uuid = UUID.randomUUID();
              final String uuidStr = uuid.toString();
              key.setId(uuidStr);
              final Urn executionRequestUrn =
                  EntityKeyUtils.convertEntityKeyToUrn(key, EXECUTION_REQUEST_ENTITY_NAME);

              // Fetch the original ingestion source
              final Urn actionPipelineUrn = Urn.createFromString(input.getActionPipelineUrn());
              final Map<Urn, EntityResponse> response =
                  _entityClient.batchGetV2(
                      ACTION_PIPELINE_ENTITY_NAME,
                      ImmutableSet.of(actionPipelineUrn),
                      ImmutableSet.of(ACTION_INFO_ASPECT_NAME),
                      context.getAuthentication());

              if (!response.containsKey(actionPipelineUrn)) {
                throw new DataHubGraphQLException(
                    String.format(
                        "Failed to find action pipeline with urn %s", actionPipelineUrn.toString()),
                    DataHubGraphQLErrorCode.BAD_REQUEST);
              }

              final EnvelopedAspect envelopedInfo =
                  response.get(actionPipelineUrn).getAspects().get(ACTION_INFO_ASPECT_NAME);
              final DataHubActionInfo actionPipelineInfo =
                  new DataHubActionInfo(envelopedInfo.getValue().data());

              if (!actionPipelineInfo.getConfig().hasRecipe()) {
                throw new DataHubGraphQLException(
                    String.format(
                        "Failed to find valid action pipeline with urn %s. Missing recipe",
                        actionPipelineUrn.toString()),
                    DataHubGraphQLErrorCode.BAD_REQUEST);
              }

              // Build the arguments map.
              final ExecutionRequestInput execInput = new ExecutionRequestInput();
              execInput.setTask(RUN_INGEST_TASK_NAME); // Set the RUN_INGEST task
              execInput.setSource(
                  new ExecutionRequestSource()
                      .setType(MANUAL_EXECUTION_SOURCE_NAME)
                      .setIngestionSource(actionPipelineUrn));
              // execInput.setExecutorId(actionPipelineInfo.getConfig().getExecutorId(),
              // SetMode.IGNORE_NULL);
              execInput.setRequestedAt(System.currentTimeMillis());

              Map<String, String> arguments = new HashMap<>();
              String recipe = actionPipelineInfo.getConfig().getRecipe();
              // recipe = injectRunId(recipe, executionRequestUrn.toString());
              // recipe = IngestionUtils.injectPipelineName(recipe, actionPipelineUrn.toString());
              arguments.put(RECIPE_ARG_NAME, recipe);
              //          arguments.put(VERSION_ARG_NAME,
              // actionPipelineInfo.getConfig().hasVersion()
              //              ? actionPipelineInfo.getConfig().getVersion()
              //              : "0.0.0dev0"
              //          );
              //          if (actionPipelineInfo.getConfig().hasVersion()) {
              //            arguments.put(VERSION_ARG_NAME,
              // actionPipelineInfo.getConfig().getVersion());
              //          }
              String debugMode = "false";
              //          if (actionPipelineInfo.getConfig().hasDebugMode()) {
              //            debugMode = actionPipelineInfo.getConfig().isDebugMode() ? "true" :
              // "false";
              //          }
              //          arguments.put(DEBUG_MODE_ARG_NAME, debugMode);
              execInput.setArgs(new StringMap(arguments));

              return _integrationsService.registerAction(actionPipelineUrn, recipe);

              //          final MetadataChangeProposal proposal =
              // buildMetadataChangeProposalWithKey(key,
              //              EXECUTION_REQUEST_ENTITY_NAME, EXECUTION_REQUEST_INPUT_ASPECT_NAME,
              // execInput);
              //          return _entityClient.ingestProposal(proposal, context.getAuthentication(),
              // false);
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to create new ingestion execution request %s", input), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  /**
   * Injects an override run id into a recipe for tracking purposes. Any existing run id will be
   * overwritten.
   *
   * <p>TODO: Determine if this should be handled in the executor itself.
   *
   * @param runId the run id to place into the recipe
   * @return a modified recipe JSON string
   */
  private String injectRunId(final String originalJson, final String runId) {
    try {
      JSONObject obj = new JSONObject(originalJson);
      obj.put("run_id", runId);
      return obj.toString();
    } catch (JSONException e) {
      // This should ideally never be hit.
      throw new IllegalArgumentException(
          "Failed to create execution request: Invalid recipe json provided.");
    }
  }
}
