package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateIngestionExecutionRequestInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.IngestionUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.MaskingManager;
import io.datahubproject.metadata.services.SecretMasker;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates an on-demand ingestion execution request. */
public class CreateIngestionExecutionRequestResolver
    implements DataFetcher<CompletableFuture<String>> {

  private static final Logger log =
      LoggerFactory.getLogger(CreateIngestionExecutionRequestResolver.class);
  private static final String RUN_INGEST_TASK_NAME = "RUN_INGEST";
  private static final String MANUAL_EXECUTION_SOURCE_NAME = "MANUAL_INGESTION_SOURCE";
  private static final String RECIPE_ARG_NAME = "recipe";
  private static final String VERSION_ARG_NAME = "version";
  private static final String DEBUG_MODE_ARG_NAME = "debug_mode";

  private final EntityClient _entityClient;
  private final IngestionConfiguration _ingestionConfiguration;

  public CreateIngestionExecutionRequestResolver(
      final EntityClient entityClient, final IngestionConfiguration ingestionConfiguration) {
    _entityClient = entityClient;
    _ingestionConfiguration = ingestionConfiguration;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Check authorization before setting up masking to avoid thread-local leaks
          if (!IngestionAuthUtils.canManageIngestion(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          final CreateIngestionExecutionRequestInput input =
              bindArgument(
                  environment.getArgument("input"), CreateIngestionExecutionRequestInput.class);

          try {
            // Fetch ingestion source once (eliminates redundant fetch)
            final Urn ingestionSourceUrn = Urn.createFromString(input.getIngestionSourceUrn());
            final DataHubIngestionSourceInfo sourceInfo =
                fetchIngestionSource(context.getOperationContext(), ingestionSourceUrn);

            // Setup masking with fetched source info
            setupSecretMasking(context.getOperationContext(), sourceInfo, ingestionSourceUrn);

            try {
              // Generate execution request ID
              final ExecutionRequestKey key = new ExecutionRequestKey();
              final UUID uuid = UUID.randomUUID();
              final String uuidStr = uuid.toString();
              key.setId(uuidStr);
              final Urn executionRequestUrn =
                  EntityKeyUtils.convertEntityKeyToUrn(key, EXECUTION_REQUEST_ENTITY_NAME);

              // Build the arguments map using already-fetched source info
              final ExecutionRequestInput execInput = new ExecutionRequestInput();
              execInput.setTask(RUN_INGEST_TASK_NAME);
              execInput.setSource(
                  new ExecutionRequestSource()
                      .setType(MANUAL_EXECUTION_SOURCE_NAME)
                      .setIngestionSource(ingestionSourceUrn));
              execInput.setExecutorId(sourceInfo.getConfig().getExecutorId(), SetMode.IGNORE_NULL);
              execInput.setRequestedAt(System.currentTimeMillis());
              execInput.setActorUrn(UrnUtils.getUrn(context.getActorUrn()));

              Map<String, String> arguments = new HashMap<>();
              String recipe = sourceInfo.getConfig().getRecipe();
              recipe = injectRunId(recipe, executionRequestUrn.toString());
              recipe = IngestionUtils.injectPipelineName(recipe, ingestionSourceUrn.toString());
              arguments.put(RECIPE_ARG_NAME, recipe);
              arguments.put(
                  VERSION_ARG_NAME,
                  sourceInfo.getConfig().hasVersion()
                      ? sourceInfo.getConfig().getVersion()
                      : _ingestionConfiguration.getDefaultCliVersion());
              if (sourceInfo.getConfig().hasVersion()) {
                arguments.put(VERSION_ARG_NAME, sourceInfo.getConfig().getVersion());
              }
              String debugMode = "false";
              if (sourceInfo.getConfig().hasDebugMode()) {
                debugMode = sourceInfo.getConfig().isDebugMode() ? "true" : "false";
              }
              if (sourceInfo.getConfig().hasExtraArgs()) {
                arguments.putAll(sourceInfo.getConfig().getExtraArgs());
              }
              arguments.put(DEBUG_MODE_ARG_NAME, debugMode);
              execInput.setArgs(new StringMap(arguments));

              final MetadataChangeProposal proposal =
                  buildMetadataChangeProposalWithKey(
                      key,
                      EXECUTION_REQUEST_ENTITY_NAME,
                      EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                      execInput);
              return _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
            } finally {
              // Always clean up masking regardless of execution success/failure
              MaskingManager.cleanupForCurrentThread();
            }
          } catch (Exception e) {
            // Ensure cleanup even if setup or early initialization fails
            MaskingManager.cleanupForCurrentThread();
            throw new RuntimeException(
                String.format("Failed to create new ingestion execution request %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Fetches ingestion source information from the metadata store.
   *
   * @param opContext The operation context with authentication
   * @param ingestionSourceUrn The URN of the ingestion source
   * @return The ingestion source information
   * @throws Exception if source not found or fetch fails
   */
  private DataHubIngestionSourceInfo fetchIngestionSource(
      OperationContext opContext, Urn ingestionSourceUrn) throws Exception {
    final Map<Urn, EntityResponse> response =
        _entityClient.batchGetV2(
            opContext,
            INGESTION_SOURCE_ENTITY_NAME,
            ImmutableSet.of(ingestionSourceUrn),
            ImmutableSet.of(INGESTION_INFO_ASPECT_NAME));

    if (!response.containsKey(ingestionSourceUrn)) {
      throw new DataHubGraphQLException(
          String.format("Failed to find ingestion source with urn %s", ingestionSourceUrn),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    final EnvelopedAspect envelopedInfo =
        response.get(ingestionSourceUrn).getAspects().get(INGESTION_INFO_ASPECT_NAME);
    final DataHubIngestionSourceInfo sourceInfo =
        new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

    if (!sourceInfo.getConfig().hasRecipe()) {
      throw new DataHubGraphQLException(
          String.format(
              "Failed to find valid ingestion source with urn %s. Missing recipe",
              ingestionSourceUrn),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    return sourceInfo;
  }

  /**
   * Sets up secret masking for the current thread. Fail-safe: continues without masking if setup
   * fails, but logs security warnings.
   *
   * @param opContext The operation context (used for structured logging)
   * @param sourceInfo The ingestion source information with recipe
   * @param ingestionSourceUrn The URN for logging context
   */
  private void setupSecretMasking(
      OperationContext opContext, DataHubIngestionSourceInfo sourceInfo, Urn ingestionSourceUrn) {
    try {
      log.debug(
          "Setting up secret masking [source={}, thread={}]",
          ingestionSourceUrn,
          Thread.currentThread().getId());

      String recipe = sourceInfo.getConfig().getRecipe();
      Set<String> envVars = SecretMasker.extractEnvVarReferences(recipe);

      if (envVars.isEmpty()) {
        log.debug(
            "No environment variables found in recipe for source '{}'. Masking not needed.",
            ingestionSourceUrn);
        return;
      }

      // Install masking for the current thread
      SecretMasker masker = new SecretMasker(envVars);
      if (!masker.isEnabled()) {
        log.error(
            "SECURITY WARNING: Failed to enable secret masking for source '{}' - "
                + "no valid secrets found despite {} variable references. "
                + "Secrets may be exposed in logs!",
            ingestionSourceUrn,
            envVars.size());
        return;
      }

      MaskingManager.installForCurrentThread(masker);

      log.info(
          "Secret masking ENABLED [source={}, variables={}, thread={}]",
          ingestionSourceUrn,
          envVars.size(),
          Thread.currentThread().getId());
      log.debug("Masking variables: {}", envVars);

    } catch (Exception e) {
      log.error(
          "SECURITY CRITICAL: Failed to set up secret masking for source '{}'. "
              + "Secrets may be exposed in logs! Error: {}",
          ingestionSourceUrn,
          e.getMessage(),
          e);
      // Consider throwing in production to fail-fast:
      // throw new SecurityException("Cannot proceed without secret masking", e);
    }
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
