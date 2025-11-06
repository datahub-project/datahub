package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateTestConnectionRequestInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates an on-demand ingestion execution request. */
public class CreateTestConnectionRequestResolver implements DataFetcher<CompletableFuture<String>> {

  private static final Logger log =
      LoggerFactory.getLogger(CreateTestConnectionRequestResolver.class);
  private static final String TEST_CONNECTION_TASK_NAME = "TEST_CONNECTION";
  private static final String TEST_CONNECTION_SOURCE_NAME = "MANUAL_TEST_CONNECTION";
  private static final String RECIPE_ARG_NAME = "recipe";
  private static final String VERSION_ARG_NAME = "version";
  private static final String DEFAULT_EXECUTOR_ID = "default";

  private final EntityClient _entityClient;
  private final IngestionConfiguration _ingestionConfiguration;

  public CreateTestConnectionRequestResolver(
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

          final CreateTestConnectionRequestInput input =
              bindArgument(
                  environment.getArgument("input"), CreateTestConnectionRequestInput.class);

          try {
            // Set up secret masking for this test connection request
            setupSecretMasking(context.getOperationContext(), input.getRecipe());

            try {
              final ExecutionRequestKey key = new ExecutionRequestKey();
              final UUID uuid = UUID.randomUUID();
              final String uuidStr = uuid.toString();
              key.setId(uuidStr);
              final Urn executionRequestUrn =
                  EntityKeyUtils.convertEntityKeyToUrn(key, EXECUTION_REQUEST_ENTITY_NAME);

              final ExecutionRequestInput execInput = new ExecutionRequestInput();
              execInput.setTask(TEST_CONNECTION_TASK_NAME);
              execInput.setSource(
                  new ExecutionRequestSource().setType(TEST_CONNECTION_SOURCE_NAME));
              execInput.setExecutorId(DEFAULT_EXECUTOR_ID);
              execInput.setRequestedAt(System.currentTimeMillis());
              execInput.setActorUrn(UrnUtils.getUrn(context.getActorUrn()));

              Map<String, String> arguments = new HashMap<>();
              arguments.put(
                  RECIPE_ARG_NAME,
                  IngestionUtils.injectPipelineName(
                      input.getRecipe(), executionRequestUrn.toString()));
              if (input.getVersion() != null) {
                arguments.put(VERSION_ARG_NAME, input.getVersion());
              }
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
                String.format(
                    "Failed to create new test ingestion connection request %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Sets up secret masking for the current thread. Fail-safe: continues without masking if setup
   * fails, but logs security warnings.
   *
   * @param opContext The operation context (used for structured logging)
   * @param recipe The ingestion recipe YAML/JSON string
   */
  private void setupSecretMasking(OperationContext opContext, String recipe) {
    try {
      log.debug(
          "Setting up secret masking [type=test_connection, thread={}]",
          Thread.currentThread().getId());

      Set<String> envVars = SecretMasker.extractEnvVarReferences(recipe);

      if (envVars.isEmpty()) {
        log.debug("No environment variables found in test connection recipe. Masking not needed.");
        return;
      }

      // Install masking for the current thread
      SecretMasker masker = new SecretMasker(envVars);
      if (!masker.isEnabled()) {
        log.error(
            "SECURITY WARNING: Failed to enable secret masking for test connection - "
                + "no valid secrets found despite {} variable references. "
                + "Secrets may be exposed in logs!",
            envVars.size());
        return;
      }

      MaskingManager.installForCurrentThread(masker);

      log.info(
          "Secret masking ENABLED [type=test_connection, variables={}, thread={}]",
          envVars.size(),
          Thread.currentThread().getId());
      log.debug("Masking variables: {}", envVars);

    } catch (Exception e) {
      log.error(
          "SECURITY CRITICAL: Failed to set up secret masking for test connection. "
              + "Secrets may be exposed in logs! Error: {}",
          e.getMessage(),
          e);
      // Consider throwing in production to fail-fast:
      // throw new SecurityException("Cannot proceed without secret masking", e);
    }
  }
}
