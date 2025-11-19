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
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
  private static final Integer NUMBER_OF_INGESTION_SOURCES_TO_CHECK = 1000;

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
    List<FacetFilterInput> filters =
        input.getFilters() == null ? new ArrayList<>() : input.getFilters();

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
            // Add additional filters to show only or hide all system ingestion sources
            addDefaultFilters(context, filters, input.getSystemSources());
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

            // Now that we have entities we can bind this to a result.
            final IngestionSourceExecutionRequests result = new IngestionSourceExecutionRequests();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setExecutionRequests(mapUnresolvedExecutionRequests(gmsResult.getEntities()));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list executions", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
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

  private void addDefaultFilters(
      final QueryContext context,
      List<FacetFilterInput> filters,
      @Nullable final Boolean systemSources)
      throws Exception {
    addAccessibleIngestionSourceFilter(context, filters, systemSources); // Saas only
  }

  /**
   * Saas only: This method adds a filter to the filters list to only include ingestion sources that
   * are accessible by the user. If the user is filtering by specific ingestion source URN(s), it
   * will only include those sources.
   */
  private void addAccessibleIngestionSourceFilter(
      QueryContext context, List<FacetFilterInput> filters, @Nullable Boolean systemSources)
      throws Exception {
    // Check if user is filtering by specific ingestion source URN(s)
    Optional<FacetFilterInput> ingestionSourceFilter =
        filters.stream()
            .filter(f -> EXECUTION_REQUEST_INGESTION_SOURCE_FIELD.equals(f.getField()))
            .findFirst();

    List<String> requestedSourceUrns = null;

    if (ingestionSourceFilter.isPresent()
        && ingestionSourceFilter.get().getValues() != null
        && !ingestionSourceFilter.get().getValues().isEmpty()) {
      requestedSourceUrns = ingestionSourceFilter.get().getValues();
      // Remove original filter which will be replaced with accessible sources only
      filters.remove(ingestionSourceFilter.get());
    }

    List<Urn> accessibleSourceUrns =
        getUrnsOfIngestionSources(context, systemSources, requestedSourceUrns);

    if (requestedSourceUrns != null && accessibleSourceUrns.isEmpty()) {
      log.warn("None of the requested ingestion sources are accessible to this user");
    }

    // Add filter with only accessible source URNs
    filters.add(
        new FacetFilterInput(
            EXECUTION_REQUEST_INGESTION_SOURCE_FIELD,
            null,
            accessibleSourceUrns.stream().map(Urn::toString).toList(),
            false,
            FilterOperator.EQUAL));
  }

  private List<Urn> getUrnsOfIngestionSources(
      final QueryContext context,
      @Nullable final Boolean systemSources,
      @Nullable final List<String> specificSourceUrns)
      throws Exception {

    List<FacetFilterInput> filters = new ArrayList<>();

    // Add systemSources filter if specified
    if (systemSources != null) {
      filters.add(
          new FacetFilterInput(
              INGESTION_SOURCE_SOURCE_TYPE_FIELD,
              null,
              ImmutableList.of(INGESTION_SOURCE_SOURCE_TYPE_SYSTEM),
              !systemSources,
              FilterOperator.EQUAL));
    }

    // Add specific URN filter if provided
    if (specificSourceUrns != null && !specificSourceUrns.isEmpty()) {
      filters.add(
          new FacetFilterInput(
              URN_FIELD_NAME, null, specificSourceUrns, false, FilterOperator.EQUAL));
    }

    final SearchResult gmsResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            Constants.INGESTION_SOURCE_ENTITY_NAME,
            DEFAULT_QUERY,
            buildFilter(filters, Collections.emptyList()),
            null,
            0,
            NUMBER_OF_INGESTION_SOURCES_TO_CHECK);

    return gmsResult.getEntities().stream().map(SearchEntity::getEntity).toList();
  }
}
