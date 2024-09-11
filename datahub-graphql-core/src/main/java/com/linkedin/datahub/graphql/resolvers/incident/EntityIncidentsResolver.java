package com.linkedin.datahub.graphql.resolvers.incident;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityIncidentsResult;
import com.linkedin.datahub.graphql.generated.Incident;
import com.linkedin.datahub.graphql.types.incident.IncidentMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** GraphQL Resolver used for fetching the list of Assertions associated with an Entity. */
public class EntityIncidentsResolver
    implements DataFetcher<CompletableFuture<EntityIncidentsResult>> {

  static final String INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME = "entities.keyword";
  static final String INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME = "state";
  static final String CREATED_TIME_SEARCH_INDEX_FIELD_NAME = "created";

  private final EntityClient _entityClient;

  public EntityIncidentsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<EntityIncidentsResult> get(DataFetchingEnvironment environment) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();

          final String entityUrn = ((Entity) environment.getSource()).getUrn();
          final Integer start = environment.getArgumentOrDefault("start", 0);
          final Integer count = environment.getArgumentOrDefault("count", 20);
          final Optional<String> maybeState = Optional.ofNullable(environment.getArgument("state"));

          try {
            // Step 1: Fetch set of incidents associated with the target entity from the Search
            // Index!
            // We use the search index so that we can easily sort by the last updated time.
            final Filter filter = buildIncidentsEntityFilter(entityUrn, maybeState);
            final List<SortCriterion> sortCriteria = buildIncidentsSortCriteria();
            final SearchResult searchResult =
                _entityClient.filter(
                    context.getOperationContext(),
                    Constants.INCIDENT_ENTITY_NAME,
                    filter,
                    sortCriteria,
                    start,
                    count);

            final List<Urn> incidentUrns =
                searchResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toList());

            // Step 2: Hydrate the incident entities
            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.INCIDENT_ENTITY_NAME,
                    new HashSet<>(incidentUrns),
                    null);

            // Step 3: Map GMS incident model to GraphQL model
            final List<EntityResponse> entityResult = new ArrayList<>();
            for (Urn urn : incidentUrns) {
              entityResult.add(entities.getOrDefault(urn, null));
            }
            final List<Incident> incidents =
                entityResult.stream()
                    .filter(Objects::nonNull)
                    .map(i -> IncidentMapper.map(context, i))
                    .collect(Collectors.toList());

            // Step 4: Package and return result
            final EntityIncidentsResult result = new EntityIncidentsResult();
            result.setCount(searchResult.getPageSize());
            result.setStart(searchResult.getFrom());
            result.setTotal(searchResult.getNumEntities());
            result.setIncidents(incidents);
            return result;
          } catch (URISyntaxException | RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve incidents from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Filter buildIncidentsEntityFilter(
      final String entityUrn, final Optional<String> maybeState) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME, entityUrn);
    maybeState.ifPresent(
        incidentState -> criterionMap.put(INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME, incidentState));
    return QueryUtils.newFilter(criterionMap);
  }

  private List<SortCriterion> buildIncidentsSortCriteria() {
    final SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField(CREATED_TIME_SEARCH_INDEX_FIELD_NAME);
    sortCriterion.setOrder(SortOrder.DESCENDING);
    return Collections.singletonList(sortCriterion);
  }
}
