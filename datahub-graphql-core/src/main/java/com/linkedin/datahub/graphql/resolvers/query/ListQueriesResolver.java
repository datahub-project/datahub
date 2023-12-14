package com.linkedin.datahub.graphql.resolvers.query;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListQueriesInput;
import com.linkedin.datahub.graphql.generated.ListQueriesResult;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListQueriesResolver implements DataFetcher<CompletableFuture<ListQueriesResult>> {

  // Visible for Testing
  static final Integer DEFAULT_START = 0;
  static final Integer DEFAULT_COUNT = 100;
  static final String DEFAULT_QUERY = "";
  static final String CREATED_AT_FIELD = "createdAt";
  static final String QUERY_SOURCE_FIELD = "source";
  static final String QUERY_ENTITIES_FIELD = "entities";

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListQueriesResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final ListQueriesInput input =
        bindArgument(environment.getArgument("input"), ListQueriesInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final SortCriterion sortCriterion =
                new SortCriterion().setField(CREATED_AT_FIELD).setOrder(SortOrder.DESCENDING);

            // First, get all Query Urns.
            final SearchResult gmsResult =
                _entityClient.search(
                    QUERY_ENTITY_NAME,
                    query,
                    buildFilters(input),
                    sortCriterion,
                    start,
                    count,
                    context.getAuthentication(),
                    new SearchFlags().setFulltext(true).setSkipHighlighting(true));

            final ListQueriesResult result = new ListQueriesResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setQueries(
                mapUnresolvedQueries(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list Queries", e);
          }
        });
  }

  // This method maps urns returned from the list endpoint into Partial Query objects which will be
  // resolved be a separate Batch resolver.
  private List<QueryEntity> mapUnresolvedQueries(final List<Urn> queryUrns) {
    final List<QueryEntity> results = new ArrayList<>();
    for (final Urn urn : queryUrns) {
      final QueryEntity unresolvedQuery = new QueryEntity();
      unresolvedQuery.setUrn(urn.toString());
      unresolvedQuery.setType(EntityType.QUERY);
      results.add(unresolvedQuery);
    }
    return results;
  }

  @Nullable
  private Filter buildFilters(@Nonnull final ListQueriesInput input) {
    final AndFilterInput criteria = new AndFilterInput();
    List<FacetFilterInput> andConditions = new ArrayList<>();

    // Optionally add a source filter.
    if (input.getSource() != null) {
      andConditions.add(
          new FacetFilterInput(
              QUERY_SOURCE_FIELD,
              null,
              ImmutableList.of(input.getSource().toString()),
              false,
              FilterOperator.EQUAL));
    }

    // Optionally add an entity type filter.
    if (input.getDatasetUrn() != null) {
      andConditions.add(
          new FacetFilterInput(
              QUERY_ENTITIES_FIELD,
              null,
              ImmutableList.of(input.getDatasetUrn()),
              false,
              FilterOperator.EQUAL));
    }

    criteria.setAnd(andConditions);
    return buildFilter(Collections.emptyList(), ImmutableList.of(criteria));
  }
}
