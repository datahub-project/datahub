package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateIngestionExecutionRequestInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.json.JSONException;
import org.json.JSONObject;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Creates an on-demand ingestion execution request.
 */
public class CreateIngestionExecutionRequestResolver implements DataFetcher<CompletableFuture<String>> {

  private static final String RUN_INGEST_TASK_NAME = "RUN_INGEST";
  private static final String MANUAL_EXECUTION_SOURCE_NAME = "MANUAL_INGESTION_SOURCE";
  private static final String RECIPE_ARG_NAME = "recipe";
  private static final String VERSION_ARG_NAME = "version";
  private static final String DEBUG_MODE_ARG_NAME = "debug_mode";

  private final EntityClient _entityClient;
  private final IngestionConfiguration _ingestionConfiguration;

  public CreateIngestionExecutionRequestResolver(final EntityClient entityClient, final IngestionConfiguration ingestionConfiguration) {
    _entityClient = entityClient;
    _ingestionConfiguration = ingestionConfiguration;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (IngestionAuthUtils.canManageIngestion(context)) {

        final CreateIngestionExecutionRequestInput input =
            bindArgument(environment.getArgument("input"), CreateIngestionExecutionRequestInput.class);

        try {

          final MetadataChangeProposal proposal = new MetadataChangeProposal();
          final ExecutionRequestKey key = new ExecutionRequestKey();
          final UUID uuid = UUID.randomUUID();
          final String uuidStr = uuid.toString();
          key.setId(uuidStr);
          final Urn executionRequestUrn = EntityKeyUtils.convertEntityKeyToUrn(key, Constants.EXECUTION_REQUEST_ENTITY_NAME);
          proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));

          // Fetch the original ingestion source
          final Urn ingestionSourceUrn = Urn.createFromString(input.getIngestionSourceUrn());
          final Map<Urn, EntityResponse> response =
              _entityClient.batchGetV2(Constants.INGESTION_SOURCE_ENTITY_NAME, ImmutableSet.of(ingestionSourceUrn),
                  ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME), context.getAuthentication());

          if (!response.containsKey(ingestionSourceUrn)) {
            throw new DataHubGraphQLException(
                String.format("Failed to find ingestion source with urn %s", ingestionSourceUrn.toString()),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }

          final EnvelopedAspect envelopedInfo = response.get(ingestionSourceUrn).getAspects().get(Constants.INGESTION_INFO_ASPECT_NAME);
          final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

          if (!ingestionSourceInfo.getConfig().hasRecipe()) {
            throw new DataHubGraphQLException(
                String.format("Failed to find valid ingestion source with urn %s. Missing recipe", ingestionSourceUrn.toString()),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }

          // Build the arguments map.
          final ExecutionRequestInput execInput = new ExecutionRequestInput();
          execInput.setTask(RUN_INGEST_TASK_NAME); // Set the RUN_INGEST task
          execInput.setSource(
              new ExecutionRequestSource().setType(MANUAL_EXECUTION_SOURCE_NAME).setIngestionSource(ingestionSourceUrn));
          execInput.setExecutorId(ingestionSourceInfo.getConfig().getExecutorId(), SetMode.IGNORE_NULL);
          execInput.setRequestedAt(System.currentTimeMillis());

          Map<String, String> arguments = new HashMap<>();
          arguments.put(RECIPE_ARG_NAME, injectRunId(ingestionSourceInfo.getConfig().getRecipe(), executionRequestUrn.toString()));
          arguments.put(VERSION_ARG_NAME, ingestionSourceInfo.getConfig().hasVersion()
              ? ingestionSourceInfo.getConfig().getVersion()
              : _ingestionConfiguration.getDefaultCliVersion()
          );
          if (ingestionSourceInfo.getConfig().hasVersion()) {
            arguments.put(VERSION_ARG_NAME, ingestionSourceInfo.getConfig().getVersion());
          }
          String debugMode = "false";
          if (ingestionSourceInfo.getConfig().hasDebugMode()) {
            debugMode = ingestionSourceInfo.getConfig().isDebugMode() ? "true" : "false";
          }
          arguments.put(DEBUG_MODE_ARG_NAME, debugMode);
          execInput.setArgs(new StringMap(arguments));

          proposal.setEntityType(Constants.EXECUTION_REQUEST_ENTITY_NAME);
          proposal.setAspectName(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
          proposal.setAspect(GenericRecordUtils.serializeAspect(execInput));
          proposal.setChangeType(ChangeType.UPSERT);

          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to create new ingestion execution request %s", input.toString()), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  /**
   * Injects an override run id into a recipe for tracking purposes. Any existing run id will be overwritten.
   *
   * TODO: Determine if this should be handled in the executor itself.
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
      throw new IllegalArgumentException("Failed to create execution request: Invalid recipe json provided.");
    }
  }
}
