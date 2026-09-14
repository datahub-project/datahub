package com.linkedin.datahub.graphql.resolvers.columnview;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.DataHubViewType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.ListColumnViewsResult;
import com.linkedin.datahub.graphql.generated.ListGlobalColumnViewsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Resolver used for listing GLOBAL Column Views (optionally for one target). */
@Slf4j
public class ListGlobalColumnViewsResolver
    implements DataFetcher<CompletableFuture<ListColumnViewsResult>> {

  private static final String CREATED_AT_FIELD = "createdAt";
  private static final String VIEW_TYPE_FIELD = "type";
  private static final String TARGET_FIELD = "target";
  private static final SortCriterion DEFAULT_SORT_CRITERION =
      new SortCriterion().setField(CREATED_AT_FIELD).setOrder(SortOrder.DESCENDING);
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListGlobalColumnViewsResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<ListColumnViewsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final ListGlobalColumnViewsInput input =
        bindArgument(environment.getArgument("input"), ListGlobalColumnViewsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
          final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
          final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
          final String target = input.getTarget() == null ? null : input.getTarget().toString();
          try {
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.DATAHUB_COLUMN_VIEW_ENTITY_NAME,
                    query,
                    buildFilters(target),
                    Collections.singletonList(DEFAULT_SORT_CRITERION),
                    start,
                    count);

            final ListColumnViewsResult result = new ListColumnViewsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setColumnViews(
                ListMyColumnViewsResolver.mapUnresolvedViews(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list global Column Views", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Filter buildFilters(@Nullable final String target) {
    final AndFilterInput globalCriteria = new AndFilterInput();
    List<FacetFilterInput> andConditions = new ArrayList<>();
    andConditions.add(
        new FacetFilterInput(
            VIEW_TYPE_FIELD,
            ImmutableList.of(DataHubViewType.GLOBAL.name()),
            false,
            FilterOperator.EQUAL));
    if (target != null) {
      andConditions.add(
          new FacetFilterInput(
              TARGET_FIELD, ImmutableList.of(target), false, FilterOperator.EQUAL));
    }
    globalCriteria.setAnd(andConditions);
    return buildFilter(Collections.emptyList(), ImmutableList.of(globalCriteria));
  }
}
