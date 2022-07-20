package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateTestConnectionRequestInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Creates an on-demand ingestion execution request.
 */
public class CreateTestConnectionRequestResolver implements DataFetcher<CompletableFuture<String>> {

  private static final String TEST_CONNECTION_TASK_NAME = "TEST_CONNECTION";
  private static final String TEST_CONNECTION_SOURCE_NAME = "MANUAL_TEST_CONNECTION";
  private static final String RECIPE_ARG_NAME = "recipe";
  private static final String VERSION_ARG_NAME = "version";
  private static final String DEFAULT_EXECUTOR_ID = "default";

  private final EntityClient _entityClient;
  private final IngestionConfiguration _ingestionConfiguration;

  public CreateTestConnectionRequestResolver(final EntityClient entityClient, final IngestionConfiguration ingestionConfiguration) {
    _entityClient = entityClient;
    _ingestionConfiguration = ingestionConfiguration;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (!IngestionAuthUtils.canManageIngestion(context)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      final CreateTestConnectionRequestInput input =
          bindArgument(environment.getArgument("input"), CreateTestConnectionRequestInput.class);

      try {

        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        final ExecutionRequestKey key = new ExecutionRequestKey();
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();
        key.setId(uuidStr);
        proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));

        final ExecutionRequestInput execInput = new ExecutionRequestInput();
        execInput.setTask(TEST_CONNECTION_TASK_NAME);
        execInput.setSource(new ExecutionRequestSource().setType(TEST_CONNECTION_SOURCE_NAME));
        execInput.setExecutorId(DEFAULT_EXECUTOR_ID);
        execInput.setRequestedAt(System.currentTimeMillis());

        Map<String, String> arguments = new HashMap<>();
        arguments.put(RECIPE_ARG_NAME, input.getRecipe());
        arguments.put(VERSION_ARG_NAME, _ingestionConfiguration.getDefaultCliVersion());
        execInput.setArgs(new StringMap(arguments));

        proposal.setEntityType(Constants.EXECUTION_REQUEST_ENTITY_NAME);
        proposal.setAspectName(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(execInput));
        proposal.setChangeType(ChangeType.UPSERT);

        return _entityClient.ingestProposal(proposal, context.getAuthentication());
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to create new test ingestion connection request %s", input.toString()), e);
      }
    });
  }
}
