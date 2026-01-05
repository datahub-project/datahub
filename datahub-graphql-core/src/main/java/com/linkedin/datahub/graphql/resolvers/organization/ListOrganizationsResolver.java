package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ListOrganizationsInput;
import com.linkedin.datahub.graphql.generated.ListOrganizationsResult;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.datahub.graphql.types.organization.mappers.OrganizationMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Resolver used for listing all Organizations defined within DataHub. */
public class ListOrganizationsResolver
    implements DataFetcher<CompletableFuture<ListOrganizationsResult>> {
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListOrganizationsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListOrganizationsResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final ListOrganizationsInput input =
              bindArgument(environment.getArgument("input"), ListOrganizationsInput.class);
          final Integer startVal = input.getStart();
          final Integer countVal = input.getCount();
          final Integer start = (startVal != null && startVal >= 0) ? startVal : DEFAULT_START;
          final Integer count = (countVal != null && countVal > 0) ? countVal : DEFAULT_COUNT;
          final String query = DEFAULT_QUERY;

          try {
            // First, get all organization Urns.
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    ORGANIZATION_ENTITY_NAME,
                    query,
                    null, // No filters for now
                    Collections.singletonList(
                        new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
                    start,
                    count);

            // Then, get and hydrate all organizations.
            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    ORGANIZATION_ENTITY_NAME,
                    new HashSet<>(
                        gmsResult.getEntities().stream()
                            .map(SearchEntity::getEntity)
                            .collect(Collectors.toList())),
                    null);

            // Now that we have entities we can bind this to a result.
            final ListOrganizationsResult result = new ListOrganizationsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setOrganizations(mapEntitiesToOrganizations(context, entities.values()));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list organizations", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static List<Organization> mapEntitiesToOrganizations(
      @Nullable QueryContext context, final Collection<EntityResponse> entities) {
    return entities.stream()
        .map(e -> OrganizationMapper.map(context, e))
        .filter(organization -> organization != null)
        .sorted(
            Comparator.comparing(
                org ->
                    org.getProperties() != null && org.getProperties().getName() != null
                        ? org.getProperties().getName()
                        : org.getUrn()))
        .collect(Collectors.toList());
  }
}
