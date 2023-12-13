package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateDomainInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for creating a new Domain on DataHub. Requires the CREATE_DOMAINS or MANAGE_DOMAINS
 * privilege.
 */
@Slf4j
@RequiredArgsConstructor
public class CreateDomainResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final CreateDomainInput input =
        bindArgument(environment.getArgument("input"), CreateDomainInput.class);
    final Urn parentDomain =
        input.getParentDomain() != null ? UrnUtils.getUrn(input.getParentDomain()) : null;

    return CompletableFuture.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canCreateDomains(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            // Create the Domain Key
            final DomainKey key = new DomainKey();

            // Take user provided id OR generate a random UUID for the domain.
            final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
            key.setId(id);

            if (_entityClient.exists(
                EntityKeyUtils.convertEntityKeyToUrn(key, DOMAIN_ENTITY_NAME),
                context.getAuthentication())) {
              throw new IllegalArgumentException("This Domain already exists!");
            }

            if (parentDomain != null
                && !_entityClient.exists(parentDomain, context.getAuthentication())) {
              throw new IllegalArgumentException("Parent Domain does not exist!");
            }

            if (DomainUtils.hasNameConflict(
                input.getName(), parentDomain, context, _entityClient)) {
              throw new DataHubGraphQLException(
                  String.format(
                      "\"%s\" already exists in this domain. Please pick a unique name.",
                      input.getName()),
                  DataHubGraphQLErrorCode.CONFLICT);
            }

            // Create the MCP
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    key,
                    DOMAIN_ENTITY_NAME,
                    DOMAIN_PROPERTIES_ASPECT_NAME,
                    mapDomainProperties(input, context));
            proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));

            String domainUrn =
                _entityClient.ingestProposal(proposal, context.getAuthentication(), false);
            OwnershipType ownershipType = OwnershipType.TECHNICAL_OWNER;
            if (!_entityService.exists(
                UrnUtils.getUrn(mapOwnershipTypeToEntity(ownershipType.name())))) {
              log.warn("Technical owner does not exist, defaulting to None ownership.");
              ownershipType = OwnershipType.NONE;
            }
            OwnerUtils.addCreatorAsOwner(
                context, domainUrn, OwnerEntityType.CORP_USER, ownershipType, _entityService);
            return domainUrn;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            log.error(
                "Failed to create Domain with id: {}, name: {}: {}",
                input.getId(),
                input.getName(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to create Domain with id: %s, name: %s",
                    input.getId(), input.getName()),
                e);
          }
        });
  }

  private DomainProperties mapDomainProperties(
      final CreateDomainInput input, final QueryContext context) {
    final DomainProperties result = new DomainProperties();
    result.setName(input.getName());
    result.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    result.setCreated(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(context.getActorUrn()))
            .setTime(System.currentTimeMillis()));
    if (input.getParentDomain() != null) {
      try {
        result.setParentDomain(Urn.createFromString(input.getParentDomain()));
      } catch (URISyntaxException e) {
        throw new RuntimeException(
            String.format("Failed to create Domain Urn from string: %s", input.getParentDomain()),
            e);
      }
    }
    return result;
  }
}
