package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.buildFilter;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.IngestionSourceExecutionRequests;
import com.linkedin.datahub.graphql.generated.ListExecutionRequestsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ListExecutionRequestsResolver
    implements DataFetcher<CompletableFuture<IngestionSourceExecutionRequests>> {

  private static final String DEFAULT_QUERY = "*";
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private static final String EXECUTION_REQUEST_INGESTION_SOURCE_FIELD = "ingestionSource";
  private static final String INGESTION_SOURCE_SOURCE_TYPE_FIELD = "sourceType";
  private static final String INGESTION_SOURCE_SOURCE_TYPE_SYSTEM = "SYSTEM";
  private static final Integer INGESTION_SOURCE_BATCH_SIZE = 1000;

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
        input.getFilters() == null ? new ArrayList<>() : input.getFilters();
    final SortCriterion sortCriterion = buildSortCriterion(input.getSort());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<Urn> sourceUrns =
                getUrnsOfIngestionSources(context.getOperationContext(), input.getSystemSources());
            SearchResult searchResult =
                fetchExecutionRequests(
                    context, query, filters, sortCriterion, start, count, sourceUrns);
            return buildResponse(searchResult);
          } catch (Exception e) {
            throw new RuntimeException("Failed to list executions", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private SortCriterion buildSortCriterion(
      @Nullable com.linkedin.datahub.graphql.generated.SortCriterion sortCriterionInput) {
    if (sortCriterionInput != null) {
      return new SortCriterion()
          .setField(sortCriterionInput.getField())
          .setOrder(SortOrder.valueOf(sortCriterionInput.getSortOrder().name()));
    }
    return new SortCriterion().setField("requestTimeMs").setOrder(SortOrder.DESCENDING);
  }

  private SearchResult fetchExecutionRequests(
      QueryContext context,
      String query,
      List<FacetFilterInput> filters,
      SortCriterion sortCriterion,
      int start,
      int count,
      List<Urn> sourceUrns)
      throws Exception {
    try {
      List<FacetFilterInput> modifiedFilters = new ArrayList<>(filters);
      modifiedFilters.add(getIngestionSourceFilter(sourceUrns));
      return searchExecutionRequests(
          context, query, modifiedFilters, sortCriterion, start, count, sourceUrns);
    } catch (Exception e) {
      log.warn(
          String.format(
              "Failed to query execution requests with filter of %d ingestion sources, retrying without filter",
              sourceUrns.size()),
          e);
      return searchExecutionRequests(
          context, query, filters, sortCriterion, start, count, sourceUrns);
    }
  }

  private SearchResult searchExecutionRequests(
      QueryContext context,
      String query,
      List<FacetFilterInput> filters,
      SortCriterion sortCriterion,
      int start,
      int count,
      List<Urn> sourceUrns)
      throws Exception {
    SearchResult result =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            Constants.EXECUTION_REQUEST_ENTITY_NAME,
            query,
            buildFilter(filters, Collections.emptyList()),
            sortCriterion != null ? List.of(sortCriterion) : null,
            start,
            count);

    log.info(
        String.format(
            "Found %d execution requests using filter with %d ingestion sources",
            result.getNumEntities(), sourceUrns.size()));
    return result;
  }

  private IngestionSourceExecutionRequests buildResponse(SearchResult searchResult) {
    final IngestionSourceExecutionRequests result = new IngestionSourceExecutionRequests();
    result.setStart(searchResult.getFrom());
    result.setCount(searchResult.getPageSize());
    result.setTotal(searchResult.getNumEntities());
    result.setExecutionRequests(mapUnresolvedExecutionRequests(searchResult.getEntities()));
    return result;
  }

  // This method maps urns returned from the list endpoint into Partial execution request objects
  // which will be resolved by a separate Batch resolver.
  private List<ExecutionRequest> mapUnresolvedExecutionRequests(
      final SearchEntityArray entityArray) {
    final List<ExecutionRequest> results = new ArrayList<>();
    for (final SearchEntity entity : entityArray) {
      final Urn urn = entity.getEntity();
      final ExecutionRequest executionRequest = new ExecutionRequest();
      executionRequest.setUrn(urn.toString());
      executionRequest.setType(EntityType.EXECUTION_REQUEST);
      results.add(executionRequest);
    }
    return results;
  }

  /**
   * SaaS only: This method adds a filter restrict execution requests to only ingestion sources
   * accessible by the current user, and filtered by system / non-system sources if specified.
   */
  private FacetFilterInput getIngestionSourceFilter(List<Urn> sourceUrns) {
    return new FacetFilterInput(
        EXECUTION_REQUEST_INGESTION_SOURCE_FIELD,
        null,
        sourceUrns.stream().map(Urn::toString).toList(),
        false,
        FilterOperator.EQUAL);
  }

  private List<Urn> getUrnsOfIngestionSources(
      final OperationContext context, @Nullable final Boolean systemSources) throws Exception {
    List<FacetFilterInput> filters =
        systemSources != null
            ? List.of(
                new FacetFilterInput(
                    INGESTION_SOURCE_SOURCE_TYPE_FIELD,
                    null,
                    ImmutableList.of(INGESTION_SOURCE_SOURCE_TYPE_SYSTEM),
                    !systemSources,
                    FilterOperator.EQUAL))
            : Collections.emptyList();

    List<Urn> allSourceUrns = new ArrayList<>();
    int start = 0;

    // Fetch first page of ingestion sources
    final SearchResult firstResult = searchIngestionSources(context, filters, start);
    int totalResults = firstResult.getNumEntities();
    log.info(
        String.format(
            "Fetching %d total ingestion sources in batches of %d",
            totalResults, INGESTION_SOURCE_BATCH_SIZE));

    List<Urn> pageUrns = firstResult.getEntities().stream().map(SearchEntity::getEntity).toList();
    allSourceUrns.addAll(pageUrns);

    log.debug(
        String.format(
            "Fetched first page of %d ingestion sources, total count: %d",
            pageUrns.size(), totalResults));

    start += INGESTION_SOURCE_BATCH_SIZE;
    while (start < totalResults && !pageUrns.isEmpty()) {
      final SearchResult searchResult = searchIngestionSources(context, filters, start);
      pageUrns = searchResult.getEntities().stream().map(SearchEntity::getEntity).toList();
      allSourceUrns.addAll(pageUrns);

      log.debug(
          String.format(
              "Fetched page of %d ingestion sources at offset %d", pageUrns.size(), start));

      start += INGESTION_SOURCE_BATCH_SIZE;
    }

    log.info(String.format("Fetched %d total ingestion source URNs", allSourceUrns.size()));
    return allSourceUrns;
  }

  private SearchResult searchIngestionSources(
      OperationContext context, List<FacetFilterInput> filters, int start) throws Exception {
    return _entityClient.search(
        context,
        Constants.INGESTION_SOURCE_ENTITY_NAME,
        DEFAULT_QUERY,
        buildFilter(filters, Collections.emptyList()),
        null,
        start,
        INGESTION_SOURCE_BATCH_SIZE);
  }
}
