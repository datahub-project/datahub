/**
 * Resolver for semantic search functionality in DataHub. Performs vector similarity search using
 * embeddings stored in OpenSearch k-NN indices.
 *
 * <p>Requirements: SemanticSearchService and embedding infrastructure (OpenSearch 2.17+ with k-NN
 * plugin).
 */
package com.linkedin.datahub.graphql.resolvers.semantic;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.SearchUtils.*;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.SearchInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.SearchFlagsInputMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.metadata.query.GroupingCriterion;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.SemanticSearchDisabledException;
import com.linkedin.metadata.search.SemanticSearchService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for resolving the 'semanticSearch' field of the Query type. Performs
 * semantic search against a specific DataHub Entity Type.
 */
@Slf4j
@RequiredArgsConstructor
public class SemanticSearchResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final SearchFlags SEARCH_RESOLVER_DEFAULTS =
      new SearchFlags()
          .setFulltext(true)
          .setMaxAggValues(20)
          .setSkipCache(false)
          .setSkipAggregates(false)
          .setSkipHighlighting(false)
          .setGroupingSpec(
              new GroupingSpec()
                  .setGroupingCriteria(
                      new GroupingCriterionArray(
                          new GroupingCriterion()
                              .setBaseEntityType(SCHEMA_FIELD_ENTITY_NAME)
                              .setGroupingEntityType(DATASET_ENTITY_NAME))));
  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final SemanticSearchService _semanticSearchService;

  @Override
  @WithSpan
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final SearchInput input = bindArgument(environment.getArgument("input"), SearchInput.class);
    final String entityName = EntityTypeMapper.getName(input.getType());
    // The query text only feeds the embedding provider (kNN); it never reaches a query_string
    // clause, so it is passed as typed. Escaping "/" would change the embedded text.
    final String query = input.getQuery();
    // A blank query embeds to a fixed sentinel vector under the classical provider, so kNN would
    // return arbitrary nearest documents instead of failing. Reject it before any service call.
    if (isBlank(query)) {
      throw new IllegalArgumentException("Semantic search requires a non-empty query");
    }

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;
    final SearchFlags searchFlags;
    com.linkedin.datahub.graphql.generated.SearchFlags inputFlags = input.getSearchFlags();
    if (inputFlags != null) {
      searchFlags = SearchFlagsInputMapper.INSTANCE.apply(context, inputFlags);
    } else {
      searchFlags = applyDefaultSearchFlags(null, query, SEARCH_RESOLVER_DEFAULTS);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            log.debug(
                "Executing semantic search. entity type {}, query {}, filters: {}, orFilters: {}, start: {}, count: {}, searchFlags: {}",
                input.getType(),
                input.getQuery(),
                input.getFilters(),
                input.getOrFilters(),
                start,
                count,
                searchFlags);

            return UrnSearchResultsMapper.map(
                context,
                _semanticSearchService.semanticSearch(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    List.of(entityName),
                    query,
                    ResolverUtils.buildFilter(input.getFilters(), input.getOrFilters()),
                    Collections.emptyList(),
                    start,
                    count));
          } catch (SemanticSearchDisabledException e) {
            throw new DataHubGraphQLException(
                "Semantic search is disabled in this environment",
                DataHubGraphQLErrorCode.BAD_REQUEST,
                e);
          } catch (Exception e) {
            log.error(
                "Failed to execute semantic search: entity type {}, query {}, filters: {}, orFilters: {}, start: {}, count: {}, searchFlags: {}",
                input.getType(),
                input.getQuery(),
                input.getFilters(),
                input.getOrFilters(),
                start,
                count,
                searchFlags);
            throw new RuntimeException(
                "Failed to execute semantic search: "
                    + String.format(
                        "entity type %s, query %s, filters: %s, orFilters: %s, start: %s, count: %s, searchFlags: %s",
                        input.getType(),
                        input.getQuery(),
                        input.getFilters(),
                        input.getOrFilters(),
                        start,
                        count,
                        searchFlags),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
