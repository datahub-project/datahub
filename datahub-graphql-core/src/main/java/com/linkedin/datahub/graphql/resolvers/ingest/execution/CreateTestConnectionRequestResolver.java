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
import com.linkedin.metadata.ingestion.CliVersionResolutionHelper;
import com.linkedin.metadata.ingestion.IngestionVersionMatrixService;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.IngestionUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Creates an on-demand "test connection" ingestion execution request.
 *
 * <p>Version resolution priority (top wins):
 *
 * <ol>
 *   <li>{@code input.version} — explicit per-request override (existing behavior)
 *   <li>{@code matrix[serverVersion][source.type]} — connector-specific version pin from {@link
 *       IngestionVersionMatrixService} when enabled
 *   <li>{@code matrix[serverVersion][source.type]._default}
 *   <li>{@link IngestionConfiguration#getDefaultCliVersion()} — workspace-wide fallback
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
  private static final String SOURCE_FIELD = "source";
  private static final String TYPE_FIELD = "type";

  private final EntityClient _entityClient;
  private final IngestionConfiguration _ingestionConfiguration;
  private final IngestionVersionMatrixService _versionMatrixService;

  /** Two-arg constructor — no per-connector version matrix is consulted. */
  public CreateTestConnectionRequestResolver(
      final EntityClient entityClient, final IngestionConfiguration ingestionConfiguration) {
    this(entityClient, ingestionConfiguration, null);
  }

  /**
   * Three-arg constructor for deployments that want matrix-aware version resolution. When {@code
   * versionMatrixService} is non-null the per-connector version matrix is consulted before falling
   * back to {@code defaultCliVersion}.
   */
  public CreateTestConnectionRequestResolver(
      final EntityClient entityClient,
      final IngestionConfiguration ingestionConfiguration,
      final IngestionVersionMatrixService versionMatrixService) {
    _entityClient = entityClient;
    _ingestionConfiguration = ingestionConfiguration;
    _versionMatrixService = versionMatrixService;
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
            // of these); the helper normalizes all three to "unset" and falls through to the
            // matrix / workspace default. See #17471 for the whitespace-only edge case.
            final CliVersionResolutionHelper.Result resolution =
                CliVersionResolutionHelper.resolve(
                    input.getVersion(),
                    extractSourceType(input.getRecipe()),
                    _versionMatrixService,
                    _ingestionConfiguration.getDefaultCliVersion(),
                    _versionMatrixService != null
                        ? _versionMatrixService.getServerVersion()
                        : null);
            if (resolution.getVersion() != null && !resolution.getVersion().isEmpty()) {
              arguments.put(VERSION_ARG_NAME, resolution.getVersion());
            }
            execInput.setArgs(new StringMap(arguments));
            execInput.setCliVersionProvenance(resolution.getStamp());

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

  /**
   * Best-effort extraction of {@code source.type} from a recipe JSON document. Returns {@code null}
   * for any malformed input — the resolver falls back to {@code defaultCliVersion} in that case
   * rather than failing the request, since a malformed recipe will surface a clearer error
   * downstream when the executor parses it.
   */
  static String extractSourceType(final String recipeJson) {
    if (recipeJson == null || recipeJson.isEmpty()) {
      return null;
    }
    try {
      JSONObject recipe = new JSONObject(recipeJson);
      if (!recipe.has(SOURCE_FIELD)) {
        return null;
      }
      JSONObject source = recipe.getJSONObject(SOURCE_FIELD);
      if (!source.has(TYPE_FIELD)) {
        return null;
      }
      String type = source.getString(TYPE_FIELD);
      return (type != null && !type.isEmpty()) ? type : null;
    } catch (JSONException e) {
      log.debug("Could not extract source.type from recipe for version-matrix lookup", e);
      return null;
    }
  }
}
