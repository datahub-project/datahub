package com.linkedin.datahub.graphql.resolvers.ingest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateIngestionExecutionRequestInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


public class CreateIngestionExecutionRequestResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public CreateIngestionExecutionRequestResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageIngestion(context)) {

      final CreateIngestionExecutionRequestInput
          input = bindArgument(environment.getArgument("input"), CreateIngestionExecutionRequestInput.class);

      // Finally, create the MetadataChangeProposal.
      final MetadataChangeProposal proposal = new MetadataChangeProposal();

      // Create the Ingestion source key --> use the display name as a unique id to ensure it's not duplicated.
      final ExecutionRequestKey key = new ExecutionRequestKey();
      final UUID uuid = UUID.randomUUID();
      final String uuidStr = uuid.toString();
      key.setId(uuidStr);
      proposal.setEntityKeyAspect(GenericAspectUtils.serializeAspect(key));

      // Fetch the ingestion source
      final Urn ingestionSourceUrn = Urn.createFromString(input.getIngestionSourceUrn());
      final Map<Urn, EntityResponse> response = _entityClient.batchGetV2(
          Constants.INGESTION_SOURCE_ENTITY_NAME,
          ImmutableSet.of(ingestionSourceUrn),
          ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME),
          context.getAuthentication());

      if (!response.containsKey(ingestionSourceUrn)) {
        throw new DataHubGraphQLException(
            String.format("Failed to find ingestion source with urn %s", ingestionSourceUrn.toString()),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }

      final EnvelopedAspect envelopedInfo = response.get(ingestionSourceUrn).getAspects().get(Constants.INGESTION_INFO_ASPECT_NAME);
      final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

      if (!ingestionSourceInfo.getConfig().hasRecipe()) {
        throw new DataHubGraphQLException(
            String.format("Failed to find valid ingestion source with urn %s", ingestionSourceUrn.toString()),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }

      // TODO: Consider secrets here.
      // TODO: Prevent multiple executions from occurring at once to prevent spam.

      // Build the arguments map.
      final ExecutionRequestInput execInput = new ExecutionRequestInput();
      execInput.setTask("RUN_INGEST"); // Set the RUN_INGEST task
      execInput.setSource(new ExecutionRequestSource().setType("INGESTION_SOURCE").setIngestionSource(ingestionSourceUrn));
      Map<String, String> arguments = ImmutableMap.of("recipe", ingestionSourceInfo.getConfig().getRecipe().getJson());
      execInput.setArgs(new StringMap(arguments));

      proposal.setEntityType(Constants.EXECUTION_REQUEST_ENTITY_NAME);
      proposal.setAspectName(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
      proposal.setAspect(GenericAspectUtils.serializeAspect(execInput));
      proposal.setChangeType(ChangeType.UPSERT);

      return CompletableFuture.supplyAsync(() -> {
        try {
          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to create new ingestion execution request %s", input.toString()), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
