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
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.DefaultEntityFiltersUtil;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.QueryEmbeddingException;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SemanticSearchDisabledException;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

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

  /** The legacy kNN search has no raw cosine floor, so it refuses one rather than ignore it. */
  static final String LEGACY_MIN_SIMILARITY =
      "SearchFlags.minSimilarity applies when semantic search runs the semantic search template,"
          + " and this request runs the legacy semantic search";

  private final SemanticSearchService _semanticSearchService;
  private final ViewService _viewService;
  private final FormService _formService;
  private final com.linkedin.entity.client.EntityClient _entityClient;

  @Override
  public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) {
    final QueryContext context = ResolverUtils.getQueryContext(environment);
    final SearchAcrossEntitiesInput input =
        bindArgument(environment.getArgument("input"), SearchAcrossEntitiesInput.class);

    final List<String> entityNames =
        getSearchEntityNames(context.getOperationContext(), input.getTypes());

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
    // Decided once per request, by the rule the template coordinator applies
    final boolean servedByTemplate =
        _semanticSearchService.isServedByTemplate(context.getOperationContext());
    if (!servedByTemplate
        && input.getSearchFlags() != null
        && input.getSearchFlags().getMinSimilarity() != null) {
      throw new ValidationException(LEGACY_MIN_SIMILARITY);
    }

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
                    ? getStructuredPropertyFacets(context, finalEntities)
                    : Collections.emptyList();

            if (servedByTemplate) {
              return templateSearch(
                  context,
                  finalEntities,
                  query,
                  baseFilter,
                  finalFilter,
                  searchFlags,
                  sortCriteria,
                  start,
                  count,
                  structuredPropertyFacets);
            }

            return UrnSearchResultsMapper.map(
                context,
                _semanticSearchService.semanticSearchAcrossEntities(
                    context.getOperationContext().withSearchFlags(flags -> searchFlags),
                    finalEntities,
                    query,
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
          } catch (DataHubGraphQLException e) {
            // Already typed, as a query embedding failure is
            throw e;
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

  /**
   * Semantic search through the semantic search template. As keyword search does, it adds the
   * document defaults (the published-document default and placement) to the caller's filter,
   * records whether that filter, with its view, had a criterion of its own, and keeps the default
   * fields out of the facets. A query it could not embed is an error naming why, or, when the
   * caller opted in, the keyword search results for the same request, marked with that reason.
   */
  private SearchResults templateSearch(
      final QueryContext context,
      final List<String> entityNames,
      final String query,
      @Nullable final Filter baseFilter,
      @Nullable final Filter callerFilter,
      @Nullable final SearchFlags searchFlags,
      final List<SortCriterion> sortCriteria,
      final int start,
      final int count,
      final List<String> facets)
      throws RemoteInvocationException {
    final boolean callerFiltered = FilterUtils.hasCriterion(callerFilter);
    final Filter filter =
        DefaultEntityFiltersUtil.applyDefaultEntityFilters(
            callerFilter, entityNames, searchFlags, context);
    final OperationContext searching =
        context
            .getOperationContext()
            .withSearchFlags(flags -> searchFlags)
            .withCallerOptionalFilterFields(
                DefaultEntityFiltersUtil.callerOptionalFilterFields(baseFilter))
            .withCallerFiltered(callerFiltered);
    SearchResult result;
    try {
      result =
          _semanticSearchService.semanticSearchAcrossEntitiesByTemplate(
              searching, entityNames, query, filter, sortCriteria, start, count, facets);
    } catch (RuntimeException e) {
      final QueryEmbeddingException failure =
          ExceptionUtils.throwableOfType(e, QueryEmbeddingException.class);
      if (failure == null) {
        throw e;
      }
      if (searchFlags == null
          || !Boolean.TRUE.equals(searchFlags.isKeywordFallbackOnEmbeddingFailure())) {
        throw new DataHubGraphQLException(failure.getMessage(), errorCode(failure), failure);
      }
      log.warn(
          "Semantic search could not embed the query ({}); serving keyword results, as the caller"
              + " opted in",
          failure.getReason(),
          failure);
      SearchTemplateMetrics.keywordFallback(searching, failure.getReason());
      // The keyword search the same request runs, as keyword search escapes it. A template the
      // caller named is not run again: it may be the vector template that just failed.
      try {
        result =
            _entityClient.searchAcrossEntities(
                searching.withSearchFlags(
                    flags -> {
                      flags.removeSearchTemplate();
                      return flags;
                    }),
                entityNames,
                ResolverUtils.escapeForwardSlash(query),
                filter,
                start,
                count,
                sortCriteria,
                facets);
      } catch (RemoteInvocationException | RuntimeException keywordFailure) {
        // The fallback's own failure sets the code: the embedding's code may invite a retry, which
        // would fail the same way. The message and the suppressed failure keep the embedding's
        // reason for the log.
        keywordFailure.addSuppressed(failure);
        throw new DataHubGraphQLException(
            failure.getMessage()
                + ", and the keyword fallback failed: "
                + keywordFailure.getMessage(),
            ExceptionUtils.throwableOfType(keywordFailure, IllegalArgumentException.class) != null
                ? DataHubGraphQLErrorCode.BAD_REQUEST
                : DataHubGraphQLErrorCode.SERVER_ERROR,
            keywordFailure);
      }
      result.getMetadata().setKeywordFallback(failure.getReason());
    }
    return UrnSearchResultsMapper.map(
        context, DefaultEntityFiltersUtil.removeDefaultFilterFieldsFromAggregations(result));
  }

  /**
   * A query embedding that ran out of time is an exceeded deadline, and one the embedding service
   * throttled or budgeted out is a transient unavailability. Any other failure stays a server
   * error, since it may not clear on a retry.
   */
  private static DataHubGraphQLErrorCode errorCode(final QueryEmbeddingException failure) {
    return switch (failure.getReason()) {
      case QueryEmbeddingException.TIMEOUT -> DataHubGraphQLErrorCode.DEADLINE_EXCEEDED;
      case QueryEmbeddingException.REJECTED,
          QueryEmbeddingException.RESERVE_EXHAUSTED,
          QueryEmbeddingException.BUDGET_EXHAUSTED ->
          DataHubGraphQLErrorCode.SERVICE_UNAVAILABLE;
      default -> DataHubGraphQLErrorCode.SERVER_ERROR;
    };
  }

  private List<String> getStructuredPropertyFacets(
      final QueryContext context, final List<String> searchedEntityNames) {
    try {
      // Fetch each property's entityTypes so facets can be scoped to the searched entity types —
      // aggregating an unrelated property across every searched index risks querying indexes where
      // the field was never mapped.
      SearchFlags searchFlags =
          new SearchFlags()
              .setSkipCache(true)
              .setFetchExtraFields(new StringArray(STRUCTURED_PROPERTY_ENTITY_TYPES_FIELD));
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
      final ObjectMapper objectMapper = context.getOperationContext().getObjectMapper();
      return result.getEntities().stream()
          .filter(
              entity ->
                  structuredPropertyAppliesToEntityTypes(
                      objectMapper,
                      entity.getExtraFields() == null
                          ? null
                          : entity.getExtraFields().get(STRUCTURED_PROPERTY_ENTITY_TYPES_FIELD),
                      searchedEntityNames))
          .map(entity -> entity.getEntity().toString())
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("Failed to retrieve structured property facets. Skipping...", e);
      return Collections.emptyList();
    }
  }
}
