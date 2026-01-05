package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.OrganizationUpdateInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.organization.OrganizationHierarchy;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateOrganizationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final OrganizationUpdateInput input =
        bindArgument(environment.getArgument("input"), OrganizationUpdateInput.class);
    final Urn organizationUrn = UrnUtils.getUrn(urnStr);

    if (!OrganizationAuthUtils.canManageOrganization(context, organizationUrn)) {
      throw new AuthorizationException(
          "Unauthorized to update organization. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Fetch existing properties using batchGetV2
            final Map<Urn, EntityResponse> entityResponses =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    ORGANIZATION_ENTITY_NAME,
                    Collections.singleton(organizationUrn),
                    Collections.singleton(ORGANIZATION_PROPERTIES_ASPECT_NAME));

            EntityResponse entityResponse = entityResponses.get(organizationUrn);
            if (entityResponse == null
                || !entityResponse.getAspects().containsKey(ORGANIZATION_PROPERTIES_ASPECT_NAME)) {
              throw new RuntimeException("Organization does not exist or has no properties");
            }

            com.linkedin.organization.OrganizationProperties existingProperties =
                new com.linkedin.organization.OrganizationProperties(
                    entityResponse
                        .getAspects()
                        .get(ORGANIZATION_PROPERTIES_ASPECT_NAME)
                        .getValue()
                        .data());

            if (existingProperties == null) {
              throw new RuntimeException("Organization does not exist or has no properties");
            }

            if (input.getName() != null) {
              existingProperties.setName(input.getName());
            }
            if (input.getDescription() != null) {
              existingProperties.setDescription(input.getDescription());
            }
            if (input.getLogoUrl() != null) {
              existingProperties.setLogoUrl(input.getLogoUrl());
            }

            final MetadataChangeProposal proposal = new MetadataChangeProposal();
            proposal.setEntityType(ORGANIZATION_ENTITY_NAME);
            proposal.setEntityUrn(organizationUrn);
            proposal.setAspectName(ORGANIZATION_PROPERTIES_ASPECT_NAME);
            proposal.setAspect(GenericRecordUtils.serializeAspect(existingProperties));
            proposal.setChangeType(ChangeType.UPSERT);

            _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

            if (input.getParentUrn() != null) {
              final OrganizationHierarchy hierarchy = new OrganizationHierarchy();
              hierarchy.setParent(UrnUtils.getUrn(input.getParentUrn()));

              final MetadataChangeProposal hierarchyProposal = new MetadataChangeProposal();
              hierarchyProposal.setEntityType(ORGANIZATION_ENTITY_NAME);
              hierarchyProposal.setEntityUrn(organizationUrn);
              hierarchyProposal.setAspectName(ORGANIZATION_HIERARCHY_ASPECT_NAME);
              hierarchyProposal.setAspect(GenericRecordUtils.serializeAspect(hierarchy));
              hierarchyProposal.setChangeType(ChangeType.UPSERT);

              _entityClient.ingestProposal(context.getOperationContext(), hierarchyProposal, false);
            }

            return true;
          } catch (Exception e) {
            log.error("Failed to update organization", e);
            throw new RuntimeException("Failed to update organization", e);
          }
        });
  }
}
