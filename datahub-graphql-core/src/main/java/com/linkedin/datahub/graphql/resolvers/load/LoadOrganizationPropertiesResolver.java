package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * GraphQL Resolver responsible for loading organization properties for organizations associated
 * with entities
 */
@Slf4j
public class LoadOrganizationPropertiesResolver
    implements DataFetcher<CompletableFuture<List<Organization>>> {

  private final EntityClient _entityClient;

  public LoadOrganizationPropertiesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<List<Organization>> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final QueryContext context = (QueryContext) environment.getContext();

            // Get the current entity which has organizations
            final Entity parentEntity = environment.getSource();

            // Get organizations list from parent entity
            @SuppressWarnings("unchecked")
            List<Organization> organizations =
                (List<Organization>)
                    environment
                        .getSource()
                        .getClass()
                        .getMethod("getOrganizations")
                        .invoke(environment.getSource());

            if (organizations == null || organizations.isEmpty()) {
              return Collections.emptyList();
            }

            // Extract URNs
            List<Urn> orgUrns =
                organizations.stream()
                    .map(
                        org -> {
                          try {
                            return Urn.createFromString(org.getUrn());
                          } catch (Exception e) {
                            log.warn("Failed to create URN from string: {}", org.getUrn(), e);
                            return null;
                          }
                        })
                    .filter(urn -> urn != null)
                    .collect(Collectors.toList());

            if (orgUrns.isEmpty()) {
              return organizations;
            }

            // Batch fetch organization entities
            Map<Urn, EntityResponse> responses =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    ORGANIZATION_ENTITY_NAME,
                    new java.util.HashSet<>(orgUrns),
                    Collections.singleton(ORGANIZATION_PROPERTIES_ASPECT_NAME));

            // Enrich organizations with properties
            return organizations.stream()
                .map(
                    org -> {
                      try {
                        Urn orgUrn = Urn.createFromString(org.getUrn());
                        EntityResponse response = responses.get(orgUrn);

                        if (response != null
                            && response
                                .getAspects()
                                .containsKey(ORGANIZATION_PROPERTIES_ASPECT_NAME)) {
                          com.linkedin.organization.OrganizationProperties props =
                              new com.linkedin.organization.OrganizationProperties(
                                  response
                                      .getAspects()
                                      .get(ORGANIZATION_PROPERTIES_ASPECT_NAME)
                                      .getValue()
                                      .data());

                          com.linkedin.datahub.graphql.generated.OrganizationProperties gqlProps =
                              new com.linkedin.datahub.graphql.generated.OrganizationProperties();
                          gqlProps.setName(props.getName());
                          if (props.hasDescription()) {
                            gqlProps.setDescription(props.getDescription());
                          }

                          org.setProperties(gqlProps);
                        }
                      } catch (Exception e) {
                        log.warn(
                            "Failed to enrich organization with properties: {}", org.getUrn(), e);
                      }
                      return org;
                    })
                .collect(Collectors.toList());

          } catch (Exception e) {
            log.error("Failed to load organization properties", e);
            return Collections.emptyList();
          }
        });
  }
}
