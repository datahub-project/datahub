package com.linkedin.datahub.graphql.resolvers.ingest.source;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceConfigInput;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceInput;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceScheduleInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubIngestionSourceKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Creates or updates an ingestion source. Requires the MANAGE_INGESTION privilege.
 */
public class UpsertIngestionSourceResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public UpsertIngestionSourceResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (IngestionAuthUtils.canManageIngestion(context)) {

        final Optional<String> ingestionSourceUrn = Optional.ofNullable(environment.getArgument("urn"));
        final UpdateIngestionSourceInput input = bindArgument(environment.getArgument("input"), UpdateIngestionSourceInput.class);

        final MetadataChangeProposal proposal = new MetadataChangeProposal();

        if (ingestionSourceUrn.isPresent()) {
          // Update existing ingestion source
          try {
            proposal.setEntityUrn(Urn.createFromString(ingestionSourceUrn.get()));
          } catch (URISyntaxException e) {
            throw new DataHubGraphQLException(
                String.format("Malformed urn %s provided.", ingestionSourceUrn.get()),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }
        } else {
          // Create new ingestion source
          // Since we are creating a new Ingestion Source, we need to generate a unique UUID.
          final UUID uuid = UUID.randomUUID();
          final String uuidStr = uuid.toString();

          // Create the Ingestion source key
          final DataHubIngestionSourceKey key = new DataHubIngestionSourceKey();
          key.setId(uuidStr);
          proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
        }

        // Create the policy info.
        final DataHubIngestionSourceInfo info = mapIngestionSourceInfo(input);
        proposal.setEntityType(Constants.INGESTION_SOURCE_ENTITY_NAME);
        proposal.setAspectName(Constants.INGESTION_INFO_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(info));
        proposal.setChangeType(ChangeType.UPSERT);

        try {
          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform update against ingestion source with urn %s", input.toString()), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  private DataHubIngestionSourceInfo mapIngestionSourceInfo(final UpdateIngestionSourceInput input) {
    final DataHubIngestionSourceInfo result = new DataHubIngestionSourceInfo();
    result.setType(input.getType());
    result.setName(input.getName());
    result.setConfig(mapConfig(input.getConfig()));
    if (input.getSchedule() != null) {
      result.setSchedule(mapSchedule(input.getSchedule()));
    }
    return result;
  }

  private DataHubIngestionSourceConfig mapConfig(final UpdateIngestionSourceConfigInput input) {
    final DataHubIngestionSourceConfig result = new DataHubIngestionSourceConfig();
    result.setRecipe(input.getRecipe());
    if (input.getVersion() != null) {
      result.setVersion(input.getVersion());
    }
    if (input.getExecutorId() != null) {
      result.setExecutorId(input.getExecutorId());
    }
    return result;
  }

  private DataHubIngestionSourceSchedule mapSchedule(final UpdateIngestionSourceScheduleInput input) {
    final DataHubIngestionSourceSchedule result = new DataHubIngestionSourceSchedule();
    result.setInterval(input.getInterval());
    result.setTimezone(input.getTimezone());
    return result;
  }
}
