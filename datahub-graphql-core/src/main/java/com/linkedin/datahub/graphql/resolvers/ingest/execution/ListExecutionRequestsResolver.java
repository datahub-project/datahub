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
  // Assumes system sources are always < 1000
  private static final Integer SYSTEM_INGESTION_SOURCES_LIMIT = 1000;

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

  /**
   * Saas only: This method adds a filter based on systemSources parameter to restrict execution
   * requests to system or non-system sources.
   */
  private void addDefaultFilters(
      final QueryContext context,
      List<FacetFilterInput> filters,
      @Nullable final Boolean systemSources)
      throws Exception {
    // Only add filter when systemSources is explicitly set
    if (systemSources == null) {
      return;
    }

    List<Urn> systemSourceUrns = getUrnsOfSystemSources(context);

    // Add filter with negation flag to toggle between system and non-system sources
    filters.add(
        new FacetFilterInput(
            EXECUTION_REQUEST_INGESTION_SOURCE_FIELD,
            null,
            systemSourceUrns.stream().map(Urn::toString).toList(),
            !systemSources,
            FilterOperator.EQUAL));
  }

  /**
   * Fetches all system ingestion sources.
   */
  private List<Urn> getUrnsOfSystemSources(final QueryContext context) throws Exception {
    List<FacetFilterInput> filters = new ArrayList<>();

    filters.add(
        new FacetFilterInput(
            INGESTION_SOURCE_SOURCE_TYPE_FIELD,
            null,
            ImmutableList.of(INGESTION_SOURCE_SOURCE_TYPE_SYSTEM),
            false,
            FilterOperator.EQUAL));

    final SearchResult gmsResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            Constants.INGESTION_SOURCE_ENTITY_NAME,
            DEFAULT_QUERY,
            buildFilter(filters, Collections.emptyList()),
            null,
            0,
            SYSTEM_INGESTION_SOURCES_LIMIT);

    return gmsResult.getEntities().stream().map(SearchEntity::getEntity).toList();
  }
}
