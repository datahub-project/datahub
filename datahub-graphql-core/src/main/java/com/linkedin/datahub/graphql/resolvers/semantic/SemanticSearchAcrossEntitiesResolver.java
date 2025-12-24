/**
 * Resolver for cross-entity semantic search functionality in DataHub. Performs vector similarity
 * search across multiple entity types using embeddings stored in OpenSearch k-NN indices.
 *
 * <p>Requirements: SemanticSearchService and embedding infrastructure (OpenSearch 2.17+ with k-NN
 * plugin).
 */
package com.linkedin.datahub.graphql.resolvers.semantic;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SemanticSearchDisabledException;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for resolving 'semanticSearchAcrossEntities' field of the Query type.
 * Performs semantic search across multiple DataHub entity types.
 */
@Slf4j
@RequiredArgsConstructor
public class SemanticSearchAcrossEntitiesResolver
    implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final int DEFAULT_START = 0;
  private static final int DEFAULT_COUNT = 10;

  private final SemanticSearchService _semanticSearchService;
  private final ViewService _viewService;
  private final FormService _formService;
  private final com.linkedin.entity.client.EntityClient _entityClient;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final SearchAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);

    final List<String> entityNames = getEntityNames(input.getTypes());

    // escape forward slash since it is a reserved character in Elasticsearch
    final String sanitizedQuery = ResolverUtils.escapeForwardSlash(input.getQuery());

    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final DataHubViewInfo maybeResolvedView =
              (input.getViewUrn() != null)
                  ? resolveView(
                      context.getOperationContext(),
                      _viewService,
                      UrnUtils.getUrn(input.getViewUrn()))
                  : null;

          final Filter baseFilter =
              ResolverUtils.buildFilter(input.getFilters(), input.getOrFilters());

          SearchFlags searchFlags = mapInputFlags(context, input.getSearchFlags());
          List<SortCriterion> sortCriteria = SearchUtils.getSortCriteria(input.getSortInput());

          try {
            log.debug(
                "Executing semantic search across entities: entity types {}, query {}, filters: {}, start: {}, count: {}",
                input.getTypes(),
                input.getQuery(),
                input.getOrFilters(),
                start,
                count);

            List<String> finalEntities =
                maybeResolvedView != null
                    ? SearchUtils.intersectEntityTypes(
                        entityNames, maybeResolvedView.getDefinition().getEntityTypes())
                    : entityNames;
            if (finalEntities.size() == 0) {
              return SearchUtils.createEmptySearchResults(start, count);
            }

            Filter finalFilter =
                maybeResolvedView != null
                    ? FilterUtils.combineFilters(
                        baseFilter, maybeResolvedView.getDefinition().getFilter())
                    : baseFilter;

            boolean shouldIncludeStructuredPropertyFacets =
                input.getSearchFlags() != null
                        && input.getSearchFlags().getIncludeStructuredPropertyFacets() != null
                    ? input.getSearchFlags().getIncludeStructuredPropertyFacets()
                    : false;
            List<String> structuredPropertyFacets =
                shouldIncludeStructuredPropertyFacets
                    ? getStructuredPropertyFacets(context)
                    : Collections.emptyList();

            return UrnSearchResultsMapper.map(
                context,
                _semanticSearchService.semanticSearchAcrossEntities(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    finalEntities,
                    sanitizedQuery,
                    finalFilter,
                    sortCriteria,
                    start,
                    count,
                    structuredPropertyFacets));
          } catch (SemanticSearchDisabledException e) {
            throw new DataHubGraphQLException(
                "Semantic search is disabled in this environment",
                DataHubGraphQLErrorCode.BAD_REQUEST,
                e);
          } catch (Exception e) {
            log.error(
                "Failed to execute semantic search across entities: entity types {}, query {}, filters: {}, start: {}, count: {}",
                input.getTypes(),
                input.getQuery(),
                input.getOrFilters(),
                start,
                count);
            throw new RuntimeException(
                "Failed to execute semantic search: "
                    + String.format(
                        "entity types %s, query %s, filters: %s, start: %s, count: %s",
                        input.getTypes(), input.getQuery(), input.getOrFilters(), start, count),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private List<String> getStructuredPropertyFacets(final QueryContext context) {
    try {
      SearchFlags searchFlags = new SearchFlags().setSkipCache(true);
      SearchResult result =
          _entityClient.searchAcrossEntities(
              context.getOperationContext().withSearchFlags(flags -> searchFlags),
              ImmutableList.of("structuredProperty"),
              "*",
              null,
              0,
              1000,
              null,
              null);
      return result.getEntities().stream()
          .map(entity -> entity.getEntity().toString())
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Failed to retrieve structured property facets. Skipping...", e);
      return Collections.emptyList();
    }
  }
}
