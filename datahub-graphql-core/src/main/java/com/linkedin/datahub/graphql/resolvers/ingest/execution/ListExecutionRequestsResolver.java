package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.Constants.URN_FIELD_NAME;
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
import graphql.GraphQLContext;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Lists execution requests scoped to <b>ingestion sources only</b>. Results are always filtered to
 * execution requests whose {@code ingestionSource} field matches an ingestion source the caller has
 * access to.
 */
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
  // Elasticsearch's default index.max_result_window: a page that ends beyond it is rejected by the
  // search backend, so ingestion sources are only collected up to this many.
  private static final int INGESTION_SOURCE_MAX_RESULT_WINDOW = 10_000;

  // Request-scoped cache key prefix for the accessible ingestion-source URN list. A single GraphQL
  // request can invoke this resolver several times (e.g. once per item of a list it is nested
  // under), and each invocation would otherwise re-scan every ingestion source in the instance. The
  // list depends only on the actor, the system-source filter and the requested ingestion sources;
  // the actor is constant within a request and the rest is part of the key, so we memoize it on
  // the GraphQLContext.
  private static final String ACCESSIBLE_INGESTION_SOURCE_URNS_CACHE_KEY =
      "ListExecutionRequestsResolver.accessibleIngestionSourceUrns:";

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
    final GraphQLContext graphQLContext = environment.getGraphQlContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<Urn> sourceUrns =
                getUrnsOfIngestionSources(
                    context.getOperationContext(),
                    graphQLContext,
                    input.getSystemSources(),
                    getRequestedIngestionSourceUrns(filters));
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
    List<FacetFilterInput> modifiedFilters = new ArrayList<>(filters);
    modifiedFilters.add(getIngestionSourceFilter(sourceUrns));
    return searchExecutionRequests(
        context, query, modifiedFilters, sortCriterion, start, count, sourceUrns);
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
   * This method adds a filter restrict execution requests to only ingestion sources accessible by
   * the current user, and filtered by system / non-system sources if specified.
   */
  private FacetFilterInput getIngestionSourceFilter(List<Urn> sourceUrns) {
    return new FacetFilterInput(
        EXECUTION_REQUEST_INGESTION_SOURCE_FIELD,
        sourceUrns.stream().map(Urn::toString).toList(),
        false,
        FilterOperator.EQUAL);
  }

  /**
   * Returns the ingestion source URNs the request explicitly selects (a non-negated equality filter
   * on {@code ingestionSource}), or null if it does not select specific sources. These are only a
   * narrowing hint: they are still checked against the accessible sources.
   */
  @Nullable
  private static List<String> getRequestedIngestionSourceUrns(List<FacetFilterInput> filters) {
    return filters.stream()
        .filter(f -> EXECUTION_REQUEST_INGESTION_SOURCE_FIELD.equals(f.getField()))
        .filter(f -> !Boolean.TRUE.equals(f.getNegated()))
        .filter(f -> f.getCondition() == null || f.getCondition() == FilterOperator.EQUAL)
        .map(FacetFilterInput::getValues)
        .filter(values -> values != null && !values.isEmpty())
        .findFirst()
        .orElse(null);
  }

  /**
   * Returns the URNs of ingestion sources accessible to the caller, memoized for the lifetime of
   * the GraphQL request. When the resolver is invoked once per item of a list, without memoization
   * each call would paginate through every ingestion source in the instance, making the cost scale
   * with items × ingestion sources. The result depends only on the actor, {@code systemSources} and
   * {@code requestedSourceUrns}; the actor is constant within a request and the rest is part of the
   * cache key.
   */
  private List<Urn> getUrnsOfIngestionSources(
      final OperationContext context,
      @Nullable final GraphQLContext graphQLContext,
      @Nullable final Boolean systemSources,
      @Nullable final List<String> requestedSourceUrns)
      throws Exception {
    if (graphQLContext == null) {
      // No request-scoped store available (e.g. unit tests); fetch directly.
      return fetchAccessibleIngestionSourceUrns(context, systemSources, requestedSourceUrns);
    }

    final String cacheKey =
        ACCESSIBLE_INGESTION_SOURCE_URNS_CACHE_KEY
            + systemSources
            + ":"
            + (requestedSourceUrns == null
                ? "*"
                : String.join(",", requestedSourceUrns.stream().sorted().toList()));
    final AtomicBoolean isCreator = new AtomicBoolean(false);
    final CompletableFuture<List<Urn>> future =
        graphQLContext.computeIfAbsent(
            cacheKey,
            k -> {
              isCreator.set(true);
              return new CompletableFuture<>();
            });

    // Only the thread that created the entry performs the fetch; concurrent callers await it. The
    // fetch runs inline on the creating thread (not a new pool task) to avoid blocking-on-submit
    // deadlocks when the GraphQL executor is saturated.
    if (isCreator.get()) {
      try {
        future.complete(
            fetchAccessibleIngestionSourceUrns(context, systemSources, requestedSourceUrns));
      } catch (Exception e) {
        // Evict the failed entry before completing it exceptionally so a sibling invocation can
        // re-attempt the scan within the same request, rather than inheriting a memoized failure.
        graphQLContext.delete(cacheKey);
        future.completeExceptionally(e);
      }
    }

    try {
      return future.join();
    } catch (CompletionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw e;
    }
  }

  /**
   * Returns the URNs of ingestion sources accessible to the caller, filtered by system / non-system
   * sources if specified. If the request selects specific ingestion sources, only those of them
   * that are accessible are returned, instead of collecting every accessible source.
   */
  private List<Urn> fetchAccessibleIngestionSourceUrns(
      final OperationContext context,
      @Nullable final Boolean systemSources,
      @Nullable final List<String> requestedSourceUrns)
      throws Exception {
    List<FacetFilterInput> filters = new ArrayList<>();
    if (systemSources != null) {
      filters.add(
          new FacetFilterInput(
              INGESTION_SOURCE_SOURCE_TYPE_FIELD,
              ImmutableList.of(INGESTION_SOURCE_SOURCE_TYPE_SYSTEM),
              !systemSources,
              FilterOperator.EQUAL));
    }
    if (requestedSourceUrns != null) {
      filters.add(
          new FacetFilterInput(URN_FIELD_NAME, requestedSourceUrns, false, FilterOperator.EQUAL));
    }

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
    while (start < totalResults
        && !pageUrns.isEmpty()
        && start + INGESTION_SOURCE_BATCH_SIZE <= INGESTION_SOURCE_MAX_RESULT_WINDOW) {
      final SearchResult searchResult = searchIngestionSources(context, filters, start);
      pageUrns = searchResult.getEntities().stream().map(SearchEntity::getEntity).toList();
      allSourceUrns.addAll(pageUrns);

      log.debug(
          String.format(
              "Fetched page of %d ingestion sources at offset %d", pageUrns.size(), start));

      start += INGESTION_SOURCE_BATCH_SIZE;
    }

    if (start < totalResults && !pageUrns.isEmpty()) {
      log.warn(
          String.format(
              "Found %d ingestion sources, more than the search result window of %d; execution"
                  + " requests are filtered by the first %d ingestion sources only",
              totalResults, INGESTION_SOURCE_MAX_RESULT_WINDOW, allSourceUrns.size()));
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
