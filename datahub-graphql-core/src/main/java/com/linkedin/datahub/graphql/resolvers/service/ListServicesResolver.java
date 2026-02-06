package com.linkedin.datahub.graphql.resolvers.service;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListServicesInput;
import com.linkedin.datahub.graphql.generated.ListServicesResult;
import com.linkedin.datahub.graphql.generated.Service;
import com.linkedin.datahub.graphql.generated.ServiceSubType;
import com.linkedin.datahub.graphql.resolvers.connection.ConnectionUtils;
import com.linkedin.datahub.graphql.types.service.ServiceType;
import com.linkedin.datahub.graphql.types.service.mappers.ServiceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Lists all Service entities present within DataHub. */
@Slf4j
public class ListServicesResolver implements DataFetcher<CompletableFuture<ListServicesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient entityClient;

  public ListServicesResolver(@Nonnull final EntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<ListServicesResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = getQueryContext(environment);
    final ListServicesInput input =
        bindArgument(environment.getArgument("input"), ListServicesInput.class);

    // Reuse MANAGE_CONNECTIONS privilege since Services are external connections
    if (!ConnectionUtils.canManageConnections(context)) {
      throw new AuthorizationException(
          "Unauthorized to list services. Please contact your DataHub administrator.");
    }

    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
    final ServiceSubType subType = input.getSubType();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // Build filter for subType if specified
            Filter filter = null;
            if (subType != null) {
              filter = buildSubTypeFilter(subType);
            }

            // Search for services
            final SearchResult searchResult =
                entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.SERVICE_ENTITY_NAME,
                    query,
                    filter,
                    ImmutableList.of(
                        new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
                    start,
                    count);

            // Batch get all entities
            final Map<Urn, EntityResponse> entities =
                entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.SERVICE_ENTITY_NAME,
                    new HashSet<>(
                        searchResult.getEntities().stream()
                            .map(SearchEntity::getEntity)
                            .collect(Collectors.toList())),
                    ServiceType.ASPECTS_TO_FETCH);

            // Build result
            final ListServicesResult result = new ListServicesResult();
            result.setStart(searchResult.getFrom());
            result.setCount(searchResult.getPageSize());
            result.setTotal(searchResult.getNumEntities());
            result.setServices(mapEntities(context, searchResult, entities));
            return result;

          } catch (Exception e) {
            throw new RuntimeException("Failed to list services", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Filter buildSubTypeFilter(ServiceSubType subType) {
    final Criterion criterion = new Criterion();
    // Use "subTypes" (plural) to match the indexed field from the SubTypes aspect
    // See SearchDocumentsResolver and RelatedDocumentsResolver for reference
    criterion.setField("subTypes");
    criterion.setValue(subType.toString());
    criterion.setCondition(Condition.EQUAL);

    final CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(criterion);

    final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    final ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    final Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }

  private List<Service> mapEntities(
      final QueryContext context,
      final SearchResult searchResult,
      final Map<Urn, EntityResponse> entities) {
    final List<Service> results = new ArrayList<>();
    for (SearchEntity searchEntity : searchResult.getEntities()) {
      final EntityResponse response = entities.get(searchEntity.getEntity());
      if (response != null) {
        final Service service = ServiceMapper.map(context, response);
        if (service != null) {
          results.add(service);
        }
      }
    }
    return results;
  }
}
