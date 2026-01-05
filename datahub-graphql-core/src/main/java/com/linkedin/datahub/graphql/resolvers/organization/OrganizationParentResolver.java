package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.datahub.graphql.types.organization.mappers.OrganizationMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.organization.OrganizationHierarchy;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

@lombok.extern.slf4j.Slf4j
public class OrganizationParentResolver implements DataFetcher<CompletableFuture<Organization>> {

  private final EntityClient _entityClient;

  public OrganizationParentResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<Organization> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urnStr =
        ((com.linkedin.datahub.graphql.generated.Organization) environment.getSource()).getUrn();
    final Urn orgUrn = UrnUtils.getUrn(urnStr);

    if (!OrganizationAuthUtils.canViewOrganization(context, orgUrn)) {
      log.debug(
          "User {} is not authorized to view parent for organization {}",
          context.getActorUrn(),
          urnStr);
      return CompletableFuture.completedFuture(null);
    }

    log.debug("Fetching parent for organization: {}", urnStr);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn urn = orgUrn;
            final EntityResponse entityResponse =
                _entityClient.getV2(
                    context.getOperationContext(),
                    ORGANIZATION_ENTITY_NAME,
                    urn,
                    Collections.singleton(ORGANIZATION_HIERARCHY_ASPECT_NAME));

            if (entityResponse != null
                && entityResponse.getAspects().containsKey(ORGANIZATION_HIERARCHY_ASPECT_NAME)) {
              final OrganizationHierarchy hierarchy =
                  new OrganizationHierarchy(
                      entityResponse
                          .getAspects()
                          .get(ORGANIZATION_HIERARCHY_ASPECT_NAME)
                          .getValue()
                          .data());
              if (hierarchy.hasParent()) {
                final Urn parentUrn = hierarchy.getParent();
                final EntityResponse parentResponse =
                    _entityClient.getV2(
                        context.getOperationContext(), ORGANIZATION_ENTITY_NAME, parentUrn, null);
                if (parentResponse != null) {
                  return OrganizationMapper.map(context, parentResponse);
                }
              }
            }
            return null;
          } catch (Exception e) {
            throw new RuntimeException("Failed to load parent organization", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
