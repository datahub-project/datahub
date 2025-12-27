package com.linkedin.datahub.graphql.resolvers.organization;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.UserOrganizations;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.organization.Organizations;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver for removing entities from organizations. */
@Slf4j
@RequiredArgsConstructor
public class RemoveEntityFromOrganizationsResolver
    implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String entityUrnStr = environment.getArgument("entityUrn");
    final List<String> organizationUrnStrs = environment.getArgument("organizationUrns");

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn entityUrn = Urn.createFromString(entityUrnStr);
            final Set<Urn> organizationUrnsToRemove =
                organizationUrnStrs.stream()
                    .map(
                        urnStr -> {
                          try {
                            return Urn.createFromString(urnStr);
                          } catch (Exception e) {
                            throw new RuntimeException("Invalid organization URN: " + urnStr, e);
                          }
                        })
                    .collect(Collectors.toSet());

            for (Urn orgUrn : organizationUrnsToRemove) {
              if (!OrganizationAuthUtils.canManageOrganizationMembers(context, orgUrn)) {
                throw new AuthorizationException(
                    "Unauthorized to remove entities from organization " + orgUrn);
              }
            }

            // Determine aspect name
            final String aspectName = getOrganizationsAspectName(entityUrn);

            // Fetch existing aspect
            final EntityResponse response =
                _entityClient.getV2(
                    context.getOperationContext(),
                    entityUrn.getEntityType(),
                    entityUrn,
                    Collections.singleton(aspectName));

            // Get current organizations
            final UrnArray currentOrganizations = getCurrentOrganizations(response, aspectName);

            // Remove specified organizations
            final Set<Urn> remainingOrganizations = new HashSet<>(currentOrganizations);
            remainingOrganizations.removeAll(organizationUrnsToRemove);

            // Update aspect
            updateOrganizationsAspect(
                context, entityUrn, aspectName, new UrnArray(remainingOrganizations));

            return true;
          } catch (Exception e) {
            log.error("Failed to remove entity from organizations", e);
            throw new RuntimeException(
                "Failed to remove entity from organizations: " + e.getMessage(), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private String getOrganizationsAspectName(final Urn entityUrn) {
    if (entityUrn.getEntityType().equals(Constants.CORP_USER_ENTITY_NAME)) {
      return Constants.USER_ORGANIZATIONS_ASPECT_NAME;
    }
    return Constants.ORGANIZATIONS_ASPECT_NAME;
  }

  private UrnArray getCurrentOrganizations(final EntityResponse response, final String aspectName) {
    if (response == null || !response.getAspects().containsKey(aspectName)) {
      return new UrnArray();
    }

    try {
      if (aspectName.equals(Constants.USER_ORGANIZATIONS_ASPECT_NAME)) {
        final UserOrganizations userOrgs =
            new UserOrganizations(response.getAspects().get(aspectName).getValue().data());
        return userOrgs.getOrganizations() != null ? userOrgs.getOrganizations() : new UrnArray();
      } else {
        final Organizations orgs =
            new Organizations(response.getAspects().get(aspectName).getValue().data());
        return orgs.getOrganizations() != null ? orgs.getOrganizations() : new UrnArray();
      }
    } catch (Exception e) {
      log.warn("Failed to parse existing organizations aspect", e);
      return new UrnArray();
    }
  }

  private void updateOrganizationsAspect(
      final QueryContext context,
      final Urn entityUrn,
      final String aspectName,
      final UrnArray organizations)
      throws Exception {

    final Object aspect;
    if (aspectName.equals(Constants.USER_ORGANIZATIONS_ASPECT_NAME)) {
      final UserOrganizations userOrgs = new UserOrganizations();
      userOrgs.setOrganizations(organizations);
      aspect = userOrgs;
    } else {
      final Organizations orgs = new Organizations();
      orgs.setOrganizations(organizations);
      aspect = orgs;
    }

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(entityUrn);
    proposal.setEntityType(entityUrn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(
        com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect(
            (com.linkedin.data.template.RecordTemplate) aspect));
    proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);

    _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
  }
}
