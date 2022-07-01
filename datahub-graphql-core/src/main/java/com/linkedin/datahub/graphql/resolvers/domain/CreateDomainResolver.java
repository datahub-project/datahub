package com.linkedin.datahub.graphql.resolvers.domain;

import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateDomainInput;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Resolver used for creating a new Domain on DataHub. Requires the CREATE_DOMAINS or MANAGE_DOMAINS privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class CreateDomainResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final CreateDomainInput input = bindArgument(environment.getArgument("input"), CreateDomainInput.class);

    return CompletableFuture.supplyAsync(() -> {

      if (!AuthorizationUtils.canCreateDomains(context)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      try {
        // Create the Domain Key
        final DomainKey key = new DomainKey();

        // Take user provided id OR generate a random UUID for the domain.
        final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
        key.setId(id);

        if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DOMAIN_ENTITY_NAME), context.getAuthentication())) {
          throw new IllegalArgumentException("This Domain already exists!");
        }

        // Create the MCP
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
        proposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
        proposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(mapDomainProperties(input)));
        proposal.setChangeType(ChangeType.UPSERT);

        return _entityClient.ingestProposal(proposal, context.getAuthentication());
      } catch (Exception e) {
        log.error("Failed to create Domain with id: {}, name: {}: {}", input.getId(), input.getName(), e.getMessage());
        throw new RuntimeException(String.format("Failed to create Domain with id: %s, name: %s", input.getId(), input.getName()), e);
      }
    });
  }

  private DomainProperties mapDomainProperties(final CreateDomainInput input) {
    final DomainProperties result = new DomainProperties();
    result.setName(input.getName());
    result.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    return result;
  }
}