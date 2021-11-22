package com.linkedin.datahub.graphql.resolvers.ingest;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.IngestionSourceKey;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

public class UpsertIngestionSourceResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public UpsertIngestionSourceResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageIngestion(context)) {

      final Optional<String> ingestionSourceUrn = Optional.ofNullable(environment.getArgument("urn"));
      final UpdateIngestionSourceInput input = bindArgument(environment.getArgument("input"), UpdateIngestionSourceInput.class);

      // Finally, create the MetadataChangeProposal.
      final MetadataChangeProposal proposal = new MetadataChangeProposal();

      if (ingestionSourceUrn.isPresent()) {
        // Update existing ingestion source
        proposal.setEntityUrn(Urn.createFromString(ingestionSourceUrn.get()));
      } else {
        // Create new ingestion source
        // Since we are creating a new Ingestion Source, we need to generate a unique UUID.
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();

        // Create the Ingestion source key
        final IngestionSourceKey key = new IngestionSourceKey();
        key.setId(uuidStr);
        proposal.setEntityKeyAspect(GenericAspectUtils.serializeAspect(key));
      }

      // Create the policy info.
      final IngestionSourceInfo info = UpdateIngestionSourceInputMapper.map(input);
      proposal.setEntityType(Constants.INGESTION_SOURCE_ENTITY_NAME);
      proposal.setAspectName("ingestionSourceInfo");
      proposal.setAspect(GenericAspectUtils.serializeAspect(info));
      proposal.setChangeType(ChangeType.UPSERT);

      return CompletableFuture.supplyAsync(() -> {
        try {
          return _entityClient.ingestProposal(proposal, context.getActor());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
