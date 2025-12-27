package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.datahub.graphql.generated.OrganizationProperties;
import com.linkedin.datahub.graphql.types.organization.mappers.OrganizationPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** GraphQL Resolver for loading organization properties when organizations are nested entities */
@Slf4j
public class OrganizationPropertiesDataFetcher
    implements DataFetcher<CompletableFuture<OrganizationProperties>> {

  private final EntityClient _entityClient;

  public OrganizationPropertiesDataFetcher(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<OrganizationProperties> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final QueryContext context = environment.getContext();
            final Organization organization = environment.getSource();

            // If properties already exist, return them
            if (organization.getProperties() != null) {
              return organization.getProperties();
            }

            // Otherwise, fetch the organization entity to get properties
            final Urn orgUrn = Urn.createFromString(organization.getUrn());

            Map<Urn, EntityResponse> responses =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    ORGANIZATION_ENTITY_NAME,
                    new HashSet<>(Collections.singleton(orgUrn)),
                    Collections.singleton(ORGANIZATION_PROPERTIES_ASPECT_NAME));

            EntityResponse response = responses.get(orgUrn);
            if (response != null
                && response.getAspects().containsKey(ORGANIZATION_PROPERTIES_ASPECT_NAME)) {
              com.linkedin.organization.OrganizationProperties props =
                  new com.linkedin.organization.OrganizationProperties(
                      response
                          .getAspects()
                          .get(ORGANIZATION_PROPERTIES_ASPECT_NAME)
                          .getValue()
                          .data());
              return OrganizationPropertiesMapper.map(context, props);
            }

            return null;
          } catch (Exception e) {
            log.error("Failed to load organization properties", e);
            return null;
          }
        });
  }
}
