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

/** Resolver for removing users from organizations. */
@Slf4j
@RequiredArgsConstructor
public class RemoveUserFromOrganizationsResolver
    implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String userUrnStr = environment.getArgument("userUrn");
    final List<String> organizationUrnStrs = environment.getArgument("organizationUrns");

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn userUrn = Urn.createFromString(userUrnStr);
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
                    "Unauthorized to remove user from organization " + orgUrn);
              }
            }

            // Fetch existing userOrganizations aspect
            final EntityResponse response =
                _entityClient.getV2(
                    context.getOperationContext(),
                    Constants.CORP_USER_ENTITY_NAME,
                    userUrn,
                    Collections.singleton(Constants.USER_ORGANIZATIONS_ASPECT_NAME));

            // Get current organizations
            UrnArray currentOrganizations = new UrnArray();
            if (response != null
                && response.getAspects().containsKey(Constants.USER_ORGANIZATIONS_ASPECT_NAME)) {
              try {
                final UserOrganizations userOrgs =
                    new UserOrganizations(
                        response
                            .getAspects()
                            .get(Constants.USER_ORGANIZATIONS_ASPECT_NAME)
                            .getValue()
                            .data());
                currentOrganizations =
                    userOrgs.getOrganizations() != null
                        ? userOrgs.getOrganizations()
                        : new UrnArray();
              } catch (Exception e) {
                log.warn("Failed to parse existing userOrganizations", e);
              }
            }

            // Remove specified organizations
            final Set<Urn> remainingOrganizations = new HashSet<>();
            if (currentOrganizations != null && currentOrganizations.size() > 0) {
              for (Urn urn : currentOrganizations) {
                remainingOrganizations.add(urn);
              }
            }
            remainingOrganizations.removeAll(organizationUrnsToRemove);

            // Update aspect
            final UserOrganizations userOrgs = new UserOrganizations();
            final UrnArray newOrganizations = new UrnArray();
            for (Urn urn : remainingOrganizations) {
              newOrganizations.add(urn);
            }
            userOrgs.setOrganizations(newOrganizations);

            final MetadataChangeProposal proposal = new MetadataChangeProposal();
            proposal.setEntityUrn(userUrn);
            proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
            proposal.setAspectName(Constants.USER_ORGANIZATIONS_ASPECT_NAME);
            proposal.setAspect(
                com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect(
                    (com.linkedin.data.template.RecordTemplate) userOrgs));
            proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);

            _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

            return true;
          } catch (Exception e) {
            log.error("Failed to remove user from organizations", e);
            throw new RuntimeException(
                "Failed to remove user from organizations: " + e.getMessage(), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
