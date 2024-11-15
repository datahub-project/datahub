package com.linkedin.datahub.graphql.resolvers.dimension;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DimensionNameEntity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ListDimensionNameInput;
import com.linkedin.datahub.graphql.generated.ListDimensionNameResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListDimensionNameResolver
    implements DataFetcher<CompletableFuture<ListDimensionNameResult>> {

  private static final String CREATED_AT_FIELD = "createdAt";
  private static final SortCriterion DEFAULT_SORT_CRITERION =
      new SortCriterion().setField(CREATED_AT_FIELD).setOrder(SortOrder.DESCENDING);

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListDimensionNameResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final ListDimensionNameInput input =
        bindArgument(environment.getArgument("input"), ListDimensionNameInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
          final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
          final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
          final List<FacetFilterInput> filters =
              input.getFilters() == null ? Collections.emptyList() : input.getFilters();

          try {
            List<SortCriterion> sortCriterionFiler = new ArrayList<>();
            sortCriterionFiler.add(DEFAULT_SORT_CRITERION);
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.DIMENSION_TYPE_ENTITY_NAME,
                    query,
                    buildFilter(filters, Collections.emptyList()),
                    sortCriterionFiler,
                    start,
                    count);

            final ListDimensionNameResult result = new ListDimensionNameResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setDimensionNames(
                mapUnresolvedDimensionNames(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list dimension names", e);
          }
        });
  }

  private List<DimensionNameEntity> mapUnresolvedDimensionNames(List<Urn> entityUrns) {
    final List<DimensionNameEntity> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final DimensionNameEntity unresolvedView = new DimensionNameEntity();
      unresolvedView.setUrn(urn.toString());
      unresolvedView.setType(EntityType.DIMENSION_NAME);
      results.add(unresolvedView);
    }
    return results;
  }
}
