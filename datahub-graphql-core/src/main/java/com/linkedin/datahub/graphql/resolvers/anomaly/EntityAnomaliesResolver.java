package com.linkedin.datahub.graphql.resolvers.anomaly;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Anomaly;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityAnomaliesResult;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** GraphQL Resolver used for fetching the list of Assertions associated with an Entity. */
public class EntityAnomaliesResolver
    implements DataFetcher<CompletableFuture<EntityAnomaliesResult>> {

  static final String ANOMALY_ENTITIES_SEARCH_INDEX_FIELD_NAME = "entity.keyword";
  static final String ANOMALY_STATE_SEARCH_INDEX_FIELD_NAME = "state";
  static final String CREATED_TIME_SEARCH_INDEX_FIELD_NAME = "created";

  private final EntityClient _entityClient;

  public EntityAnomaliesResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<EntityAnomaliesResult> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();

          final String entityUrn = ((Entity) environment.getSource()).getUrn();
          final Integer start = environment.getArgumentOrDefault("start", 0);
          final Integer count = environment.getArgumentOrDefault("count", 20);
          final Optional<String> maybeState = Optional.ofNullable(environment.getArgument("state"));

          try {
            // Step 1: Fetch set of anomalies associated with the target entity from the Search
            // Index!
            // We use the search index so that we can easily sort by the last updated time.
            final Filter filter = buildAnomaliesEntityFilter(entityUrn, maybeState);
            final List<SortCriterion> sortCriteria = buildAnomaliesSortCriteria();
            final SearchResult searchResult =
                _entityClient.filter(
                    context.getOperationContext(),
                    Constants.ANOMALY_ENTITY_NAME,
                    filter,
                    sortCriteria,
                    start,
                    count);
            final List<Urn> anomalyUrns =
                searchResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toList());
            final List<Anomaly> anomalies =
                anomalyUrns.stream()
                    .map(urn -> UrnToEntityMapper.map(context, urn))
                    .map(entity -> (Anomaly) entity)
                    .collect(Collectors.toList());
            // Step 4: Package and return result
            final EntityAnomaliesResult result = new EntityAnomaliesResult();
            result.setCount(searchResult.getPageSize());
            result.setStart(searchResult.getFrom());
            result.setTotal(searchResult.getNumEntities());
            result.setAnomalies(anomalies);
            return result;
          } catch (RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve anomalies from GMS", e);
          }
        });
  }

  private Filter buildAnomaliesEntityFilter(
      final String entityUrn, final Optional<String> maybeState) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(ANOMALY_ENTITIES_SEARCH_INDEX_FIELD_NAME, entityUrn);
    maybeState.ifPresent(
        anomalyState -> criterionMap.put(ANOMALY_STATE_SEARCH_INDEX_FIELD_NAME, anomalyState));
    return QueryUtils.newFilter(criterionMap);
  }

  private List<SortCriterion> buildAnomaliesSortCriteria() {
    final SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField(CREATED_TIME_SEARCH_INDEX_FIELD_NAME);
    sortCriterion.setOrder(SortOrder.DESCENDING);
    return Collections.singletonList(sortCriterion);
  }
}
