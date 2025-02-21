package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.buildFilter;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.IngestionSourceExecutionRequests;
import com.linkedin.datahub.graphql.generated.ListExecutionRequestsInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionResolverUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListExecutionRequestsResolver
    implements DataFetcher<CompletableFuture<IngestionSourceExecutionRequests>> {

  private static final String DEFAULT_QUERY = "*";
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<IngestionSourceExecutionRequests> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    final ListExecutionRequestsInput input =
        bindArgument(environment.getArgument("input"), ListExecutionRequestsInput.class);
    final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
    final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
    final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
    final List<FacetFilterInput> filters =
        input.getFilters() == null ? Collections.emptyList() : input.getFilters();

    // construct sort criteria, defaulting to systemCreated
    final SortCriterion sortCriterion;

    // if query is expecting to sort by something, use that
    final com.linkedin.datahub.graphql.generated.SortCriterion sortCriterionInput = input.getSort();
    if (sortCriterionInput != null) {
      sortCriterion =
          new SortCriterion()
              .setField(sortCriterionInput.getField())
              .setOrder(SortOrder.valueOf(sortCriterionInput.getSortOrder().name()));
    } else {
      sortCriterion = new SortCriterion().setField("requestTimeMs").setOrder(SortOrder.DESCENDING);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // First, get all execution request Urns.
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.EXECUTION_REQUEST_ENTITY_NAME,
                    query,
                    buildFilter(filters, Collections.emptyList()),
                    sortCriterion != null ? List.of(sortCriterion) : null,
                    start,
                    count);

            log.info(String.format("Found %d execution requests", gmsResult.getNumEntities()));

            final List<Urn> entitiesUrnList =
                gmsResult.getEntities().stream().map(SearchEntity::getEntity).toList();
            // Then, resolve all execution requests
            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.EXECUTION_REQUEST_ENTITY_NAME,
                    new HashSet<>(entitiesUrnList),
                    ImmutableSet.of(
                        Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                        Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME));
            final List<EntityResponse> entitiesOrdered =
                entitiesUrnList.stream().map(entities::get).filter(Objects::nonNull).toList();
            // Now that we have entities we can bind this to a result.
            final IngestionSourceExecutionRequests result = new IngestionSourceExecutionRequests();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setExecutionRequests(
                IngestionResolverUtils.mapExecutionRequests(context, entitiesOrdered));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list executions", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
