package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateOrganizationInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.key.OrganizationKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.organization.OrganizationHierarchy;
import com.linkedin.organization.OrganizationProperties;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver used for creating a new Organization on DataHub. */
@Slf4j
@RequiredArgsConstructor
public class CreateOrganizationResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final CreateOrganizationInput input =
        bindArgument(environment.getArgument("input"), CreateOrganizationInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!OrganizationAuthUtils.canCreateOrganization(context)) {
            throw new AuthorizationException(
                "Unauthorized to create organizations. Please contact your DataHub administrator.");
          }

          try {
            // Create the Organization Key
            final OrganizationKey key = new OrganizationKey();

            // Take user provided id OR generate a random UUID for the organization.
            final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
            key.setId(id);

            if (_entityClient.exists(
                context.getOperationContext(),
                EntityKeyUtils.convertEntityKeyToUrn(key, ORGANIZATION_ENTITY_NAME))) {
              throw new IllegalArgumentException("This Organization already exists!");
            }

            // Create the MCP
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    key,
                    ORGANIZATION_ENTITY_NAME,
                    ORGANIZATION_PROPERTIES_ASPECT_NAME,
                    mapOrganizationProperties(input, context));
            proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));

            String organizationUrn =
                _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

            // Assign ownership to the creator
            final Ownership ownership = new Ownership();
            ownership.setOwners(new OwnerArray());
            final Owner owner = new Owner();
            owner.setOwner(UrnUtils.getUrn(context.getActorUrn()));
            owner.setType(OwnershipType.TECHNICAL_OWNER);
            ownership.getOwners().add(owner);
            ownership.setLastModified(
                new AuditStamp()
                    .setActor(UrnUtils.getUrn(context.getActorUrn()))
                    .setTime(System.currentTimeMillis()));

            final MetadataChangeProposal ownershipProposal = new MetadataChangeProposal();
            ownershipProposal.setEntityType(ORGANIZATION_ENTITY_NAME);
            ownershipProposal.setEntityUrn(UrnUtils.getUrn(organizationUrn));
            ownershipProposal.setAspectName(OWNERSHIP_ASPECT_NAME);
            ownershipProposal.setAspect(GenericRecordUtils.serializeAspect(ownership));
            ownershipProposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);

            _entityClient.ingestProposal(context.getOperationContext(), ownershipProposal, false);

            // Handle Hierarchy
            if (input.getParentUrn() != null) {
              final OrganizationHierarchy hierarchy = new OrganizationHierarchy();
              hierarchy.setParent(UrnUtils.getUrn(input.getParentUrn()));

              final MetadataChangeProposal hierarchyProposal = new MetadataChangeProposal();
              hierarchyProposal.setEntityType(ORGANIZATION_ENTITY_NAME);
              hierarchyProposal.setEntityUrn(UrnUtils.getUrn(organizationUrn));
              hierarchyProposal.setAspectName(ORGANIZATION_HIERARCHY_ASPECT_NAME);
              hierarchyProposal.setAspect(GenericRecordUtils.serializeAspect(hierarchy));
              hierarchyProposal.setChangeType(ChangeType.UPSERT);

              _entityClient.ingestProposal(context.getOperationContext(), hierarchyProposal, false);
            }

            // Enforce self-membership for multi-tenant isolation
            final com.linkedin.organization.Organizations organizations =
                new com.linkedin.organization.Organizations();
            organizations.setOrganizations(
                new com.linkedin.common.UrnArray(
                    java.util.Collections.singletonList(UrnUtils.getUrn(organizationUrn))));

            final MetadataChangeProposal orgsProposal = new MetadataChangeProposal();
            orgsProposal.setEntityType(ORGANIZATION_ENTITY_NAME);
            orgsProposal.setEntityUrn(UrnUtils.getUrn(organizationUrn));
            orgsProposal.setAspectName(ORGANIZATIONS_ASPECT_NAME);
            orgsProposal.setAspect(GenericRecordUtils.serializeAspect(organizations));
            orgsProposal.setChangeType(ChangeType.UPSERT);

            _entityClient.ingestProposal(context.getOperationContext(), orgsProposal, false);

            log.info("Created organization with URN: {}", organizationUrn);
            return organizationUrn;
          } catch (Exception e) {
            log.error(
                "Failed to create Organization with id: {}, name: {}: {}",
                input.getId(),
                input.getName(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to create Organization with id: %s, name: %s",
                    input.getId(), input.getName()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private OrganizationProperties mapOrganizationProperties(
      final CreateOrganizationInput input, final QueryContext context) {
    final OrganizationProperties result = new OrganizationProperties();
    result.setName(input.getName());
    result.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    result.setCreated(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(context.getActorUrn()))
            .setTime(System.currentTimeMillis()));
    if (input.getLogoUrl() != null) {
      result.setLogoUrl(input.getLogoUrl());
    }
    return result;
  }
}
