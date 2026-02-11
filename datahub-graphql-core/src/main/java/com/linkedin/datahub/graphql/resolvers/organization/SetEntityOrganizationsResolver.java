package com.linkedin.datahub.graphql.resolvers.organization;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.UserOrganizations;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.organization.Organizations;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver for setting (replacing) organizations for an entity. */
@Slf4j
@RequiredArgsConstructor
public class SetEntityOrganizationsResolver implements DataFetcher<CompletableFuture<Boolean>> {
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
            final UrnArray organizationUrns =
                new UrnArray(
                    organizationUrnStrs.stream()
                        .map(
                            urnStr -> {
                              try {
                                return Urn.createFromString(urnStr);
                              } catch (Exception e) {
                                throw new RuntimeException(
                                    "Invalid organization URN: " + urnStr, e);
                              }
                            })
                        .collect(Collectors.toList()));

            for (Urn orgUrn : organizationUrns) {
              if (!OrganizationAuthUtils.canManageOrganizationMembers(context, orgUrn)) {
                throw new AuthorizationException(
                    "Unauthorized to set entities for organization " + orgUrn);
              }
            }

            // Determine aspect name
            final String aspectName = getOrganizationsAspectName(entityUrn);

            // Set organizations (replaces existing)
            updateOrganizationsAspect(context, entityUrn, aspectName, organizationUrns);

            return true;
          } catch (Exception e) {
            log.error("Failed to set entity organizations", e);
            throw new RuntimeException("Failed to set entity organizations: " + e.getMessage(), e);
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
