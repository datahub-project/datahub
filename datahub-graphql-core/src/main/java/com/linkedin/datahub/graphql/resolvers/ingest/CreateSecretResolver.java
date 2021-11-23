package com.linkedin.datahub.graphql.resolvers.ingest;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateSecretInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubSecretKey;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

public class CreateSecretResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public CreateSecretResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageSecrets(context)) {

      final CreateSecretInput input = bindArgument(environment.getArgument("input"), CreateSecretInput.class);

      // Finally, create the MetadataChangeProposal.
      final MetadataChangeProposal proposal = new MetadataChangeProposal();

      // Create a new DataHub secret. The secret name is part of the urn, and cannot be changed post creation.

      // Create the Ingestion source key --> use the display name as a unique id to ensure it's not duplicated.
      final DataHubSecretKey key = new DataHubSecretKey();
      key.setId(input.getDisplayName());
      proposal.setEntityKeyAspect(GenericAspectUtils.serializeAspect(key));

      // Create the policy info.
      final DataHubSecretValue value = new DataHubSecretValue();
      value.setDisplayName(input.getDisplayName());
      value.setValue(input.getValue()); // TODO: Add encryption here.

      proposal.setEntityType(Constants.SECRETS_ENTITY_NAME);
      proposal.setAspectName(Constants.SECRET_VALUE_ASPECT_NAME);
      proposal.setAspect(GenericAspectUtils.serializeAspect(value));
      proposal.setChangeType(ChangeType.UPSERT);

      return CompletableFuture.supplyAsync(() -> {
        try {
          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
