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
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.ingestion.IngestionCliVersionResolutionHelper;
import com.linkedin.metadata.ingestion.IngestionCliVersionResolutionLogger;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.IngestionUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates an on-demand "test connection" ingestion execution request.
 *
 * <p>Version resolution priority (top wins):
 *
 * <ol>
 *   <li>{@code input.version} — explicit per-request override (existing behavior)
 *   <li>{@code matrix[serverVersion][source.type]} — connector-specific version pin from {@link
 *       IngestionCliVersionMatrixService} when enabled
 *   <li>{@code matrix[serverVersion][source.type]._default}
 *   <li>{@link IngestionConfiguration#getDefaultCliVersion()} — application-wide fallback
 * </ol>
 *
 * <p>Prior to this change the test-connection path silently omitted {@code version} when the input
 * did not provide one, causing the executor to fall back to whatever bundled default it shipped
 * with — different from the path that real (non-test) executions take. The {@code
 * defaultCliVersion} fallback below closes that gap; the matrix lookup brings test connections onto
 * the same per-connector-pin behavior real executions get.
 */
@Slf4j
public class CreateTestConnectionRequestResolver implements DataFetcher<CompletableFuture<String>> {

  private static final String TEST_CONNECTION_TASK_NAME = "TEST_CONNECTION";
  private static final String TEST_CONNECTION_SOURCE_NAME = "MANUAL_TEST_CONNECTION";
  private static final String RECIPE_ARG_NAME = "recipe";
  private static final String VERSION_ARG_NAME = "version";
  private static final String DEFAULT_EXECUTOR_ID = "default";

  private final EntityClient _entityClient;
  private final IngestionConfiguration _ingestionConfiguration;
  private final IngestionCliVersionMatrixService _versionMatrixService;

  public CreateTestConnectionRequestResolver(
      final EntityClient entityClient,
      final IngestionConfiguration ingestionConfiguration,
      final IngestionCliVersionMatrixService versionMatrixService) {
    _entityClient = entityClient;
    _ingestionConfiguration = ingestionConfiguration;
    // Always a wired Spring bean (NoOp-backed when no matrix backend is configured), never null.
    _versionMatrixService = Objects.requireNonNull(versionMatrixService);
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!IngestionAuthUtils.canManageIngestion(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          final CreateTestConnectionRequestInput input =
              bindArgument(
                  environment.getArgument("input"), CreateTestConnectionRequestInput.class);

          try {
            final ExecutionRequestKey key = new ExecutionRequestKey();
            final UUID uuid = UUID.randomUUID();
            final String uuidStr = uuid.toString();
            key.setId(uuidStr);
            final Urn executionRequestUrn =
                EntityKeyUtils.convertEntityKeyToUrn(key, EXECUTION_REQUEST_ENTITY_NAME);

            final ExecutionRequestInput execInput = new ExecutionRequestInput();
            execInput.setTask(TEST_CONNECTION_TASK_NAME);
            execInput.setSource(new ExecutionRequestSource().setType(TEST_CONNECTION_SOURCE_NAME));
            execInput.setExecutorId(DEFAULT_EXECUTOR_ID);
            execInput.setRequestedAt(System.currentTimeMillis());
            execInput.setActorUrn(UrnUtils.getUrn(context.getActorUrn()));

            Map<String, String> arguments = new HashMap<>();
            arguments.put(
                RECIPE_ARG_NAME,
                IngestionUtils.injectPipelineName(
                    input.getRecipe(), executionRequestUrn.toString()));
            // input.getVersion() may be null, empty, or whitespace-only (UI forms can submit any
            // of these — an unfilled "version" field commonly renders as a 3-space string). The
            // helper normalizes all three to "unset" so resolution falls through to the matrix /
            // application default; without that normalization the blank would forward verbatim to
            // the executor and silently pin to its bundled CLI.
            final String connectorType =
                IngestionUtils.extractSourceType(
                    context.getOperationContext().getObjectMapper(), input.getRecipe());
            final IngestionCliVersionResolutionHelper.Result resolution =
                IngestionCliVersionResolutionHelper.resolve(
                    input.getVersion(),
                    connectorType,
                    _versionMatrixService,
                    _ingestionConfiguration.getDefaultCliVersion());
            if (resolution.getVersion() != null && !resolution.getVersion().isEmpty()) {
              arguments.put(VERSION_ARG_NAME, resolution.getVersion());
            }
            execInput.setArgs(new StringMap(arguments));
            execInput.setCliVersionAudit(resolution.getStamp());
            IngestionCliVersionResolutionLogger.log(
                log,
                IngestionCliVersionResolutionLogger.TRIGGER_TEST_CONNECTION,
                resolution,
                connectorType,
                IngestionCliVersionResolutionLogger.IDENTIFIER_EXECUTION_REQUEST,
                executionRequestUrn.toString());

            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    key,
                    EXECUTION_REQUEST_ENTITY_NAME,
                    EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                    execInput);
            return _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to create new test ingestion connection request %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
