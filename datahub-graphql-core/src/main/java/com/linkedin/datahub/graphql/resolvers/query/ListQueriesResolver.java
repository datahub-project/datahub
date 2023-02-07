package com.linkedin.datahub.graphql.resolvers.query;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListQueriesInput;
import com.linkedin.datahub.graphql.generated.ListQueriesResult;
import com.linkedin.datahub.graphql.types.query.QueryMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class ListQueriesResolver implements DataFetcher<CompletableFuture<ListQueriesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 100;
  private static final String DEFAULT_QUERY = "";
  private static final String LAST_MODIFIED_AT_FIELD = "lastModifiedAt";
  private static final String QUERY_SOURCE_FIELD = "source";
  private static final String QUERY_ENTITIES_FIELD = "entities";

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListQueriesResult> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();

    final ListQueriesInput input = bindArgument(environment.getArgument("input"), ListQueriesInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

    return CompletableFuture.supplyAsync(() -> {
      try {
        final SortCriterion sortCriterion =
            new SortCriterion().setField(LAST_MODIFIED_AT_FIELD).setOrder(SortOrder.DESCENDING);

        // First, get all Query Urns.
        final SearchResult gmsResult = _entityClient.search(QUERY_ENTITY_NAME, query, buildFilters(input), sortCriterion, start, count,
            context.getAuthentication(), true);

        // Then, get and hydrate all Queries.
        final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(QUERY_ENTITY_NAME,
            new HashSet<>(gmsResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList())),
            null, authentication);

        final ListQueriesResult result = new ListQueriesResult();
        result.setStart(gmsResult.getFrom());
        result.setCount(gmsResult.getPageSize());
        result.setTotal(gmsResult.getNumEntities());
        result.setQueries(entities.values().stream().map(QueryMapper::map).collect(Collectors.toList()));
        return result;
      } catch (Exception e) {
        throw new RuntimeException("Failed to list Queries", e);
      }
    });
  }

  @Nullable
  private Filter buildFilters(@Nonnull final ListQueriesInput input) {
    final AndFilterInput criteria = new AndFilterInput();
    List<FacetFilterInput> andConditions = new ArrayList<>();

    // Optionally add a source filter.
    if (input.getSource() != null) {
      andConditions.add(
          new FacetFilterInput(QUERY_SOURCE_FIELD, null, ImmutableList.of(input.getSource().toString()), false, FilterOperator.EQUAL));
    }

    // Optionally add an entity type filter.
    if (input.getDatasetUrn() != null) {
      andConditions.add(
          new FacetFilterInput(QUERY_ENTITIES_FIELD, null, ImmutableList.of(input.getDatasetUrn()), false, FilterOperator.EQUAL));
    }

    criteria.setAnd(andConditions);
    return buildFilter(Collections.emptyList(), ImmutableList.of(criteria));
  }
}
