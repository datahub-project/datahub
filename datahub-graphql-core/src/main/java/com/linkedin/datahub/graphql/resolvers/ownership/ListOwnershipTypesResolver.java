package com.linkedin.datahub.graphql.resolvers.ownership;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.ListOwnershipTypesInput;
import com.linkedin.datahub.graphql.generated.ListOwnershipTypesResult;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
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
public class ListOwnershipTypesResolver
    implements DataFetcher<CompletableFuture<ListOwnershipTypesResult>> {

  private static final String CREATED_AT_FIELD = "createdAt";
  private static final SortCriterion DEFAULT_SORT_CRITERION =
      new SortCriterion().setField(CREATED_AT_FIELD).setOrder(SortOrder.DESCENDING);

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<ListOwnershipTypesResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final ListOwnershipTypesInput input =
        bindArgument(environment.getArgument("input"), ListOwnershipTypesInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
          final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
          final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
          final List<FacetFilterInput> filters =
              input.getFilters() == null ? Collections.emptyList() : input.getFilters();

          try {

            final SearchResult gmsResult =
                _entityClient.search(
                    Constants.OWNERSHIP_TYPE_ENTITY_NAME,
                    query,
                    buildFilter(filters, Collections.emptyList()),
                    DEFAULT_SORT_CRITERION,
                    start,
                    count,
                    context.getAuthentication(),
                    new SearchFlags().setFulltext(true));

            final ListOwnershipTypesResult result = new ListOwnershipTypesResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setOwnershipTypes(
                mapUnresolvedOwnershipTypes(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list custom ownership types", e);
          }
        });
  }

  private List<OwnershipTypeEntity> mapUnresolvedOwnershipTypes(List<Urn> entityUrns) {
    final List<OwnershipTypeEntity> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final OwnershipTypeEntity unresolvedView = new OwnershipTypeEntity();
      unresolvedView.setUrn(urn.toString());
      unresolvedView.setType(EntityType.CUSTOM_OWNERSHIP_TYPE);
      results.add(unresolvedView);
    }
    return results;
  }
}
