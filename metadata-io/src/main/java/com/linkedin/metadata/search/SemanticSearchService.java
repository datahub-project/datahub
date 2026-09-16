package com.linkedin.metadata.search;

import static com.linkedin.metadata.search.utils.QueryUtils.filterEntitiesForSearch;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service layer for semantic search operations.
 *
 * <p>Contains all semantic search logic that was previously in SearchService. This separation
 * allows for better architectural isolation between keyword and semantic search.
 */
@Slf4j
public class SemanticSearchService {

  private final CachingEntitySearchService cachingEntitySearchService;
  private final EntityDocCountCache entityDocCountCache;
  private final SearchServiceConfiguration searchServiceConfig;
  private final SemanticEntitySearch semanticEntitySearchService;

  public SemanticSearchService(
      EntityDocCountCache entityDocCountCache,
      CachingEntitySearchService cachingEntitySearchService,
      SemanticEntitySearch semanticEntitySearchService,
      SearchServiceConfiguration searchServiceConfig) {
    this.cachingEntitySearchService = cachingEntitySearchService;
    this.entityDocCountCache = entityDocCountCache;
    this.semanticEntitySearchService = semanticEntitySearchService;
    this.searchServiceConfig = searchServiceConfig;
  }

  /**
   * Performs semantic search across multiple entity types.
   *
   * <p>Flow: 1) Execute semantic search against the semantic indices using {@code
   * semanticEntitySearchService}. 2) Preserve kNN order by re-ranking results with {@link
   * SimpleRanker} (a no-op over backend score). 3) If facets are requested (non-empty {@code
   * facets} list) and the incoming {@code SearchFlags} do not skip aggregates, fetch keyword facets
   * (aggregations-only) by issuing a size=0 keyword search with explicit flags (fulltext=true,
   * skipHighlighting=true, skipAggregates=false, includeDefaultFacets=true, and {@code
   * maxAggValues} propagated when present). 4) Attach the fetched aggregations and then add legacy
   * "entity" aggregates via {@link #withAdditionalAggregates(SearchEntityArray,
   * AggregationMetadataArray, List)}.
   *
   * @param opContext session-scoped {@link OperationContext}; incoming {@code SearchFlags} are read
   *     to honor caller preferences (e.g., {@code skipAggregates}, {@code maxAggValues}).
   * @param entityNames list of entity types to search in semantic indices
   * @param input user query string used to generate embeddings and execute kNN.
   * @param postFilters optional filters to apply; forwarded to both semantic search and facet
   *     fetch.
   * @param sortCriteria optional list of sort criteria; only the first element is forwarded to the
   *     semantic service (ignored for facet fetch).
   * @param from zero-based starting offset for pagination (applies to semantic results only).
   * @param size requested page size; respected by semantic search. Facet fetch uses size=0.
   * @param facets list of facet names to aggregate. When empty, no facet fetch is executed; when
   *     non-empty and {@code skipAggregates} is false, a keyword aggregations-only call is issued
   *     and attached to the semantic result.
   * @return a {@link SearchResult} containing semantic-ranked entities and, when applicable,
   *     keyword-derived facets and additional legacy aggregates.
   */
  @Nonnull
  public SearchResult semanticSearchAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    size = ConfigUtils.applyLimit(searchServiceConfig, size);
    List<String> entitiesToSearch = getEntitiesToSearch(opContext, entityNames, size);
    if (entitiesToSearch.isEmpty()) {
      // Optimization: If the indices are all empty, return empty result
      return getEmptySearchResult(from, size);
    }

    return performSemanticSearch(
        opContext, entitiesToSearch, input, postFilters, sortCriteria, from, size, facets);
  }

  /**
   * Performs semantic search against a specific entity type.
   *
   * @param opContext session-scoped {@link OperationContext}
   * @param entityNames list of entity types to search (typically one for single-entity search)
   * @param input user query string used to generate embeddings and execute kNN.
   * @param postFilters optional filters to apply
   * @param sortCriteria optional list of sort criteria
   * @param from zero-based starting offset for pagination
   * @param size requested page size
   * @return a {@link SearchResult} containing semantic-ranked entities
   */
  @Nonnull
  public SearchResult semanticSearch(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    // Single entity semantic search doesn't need facets by default
    return semanticSearchAcrossEntities(
        opContext, entityNames, input, postFilters, sortCriteria, from, size, List.of());
  }

  /** Core semantic search implementation. */
  @Nonnull
  private SearchResult performSemanticSearch(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    // Fail-fast gating: ensure semantic search is enabled and service is present
    if (!Boolean.TRUE.equals(searchServiceConfig.isSemanticSearchEnabled())) {
      throw new SemanticSearchDisabledException();
    }
    Objects.requireNonNull(
        semanticEntitySearchService, "SemanticEntitySearch service must be configured");

    // Determine facet request intent and flags up-front
    final SearchFlags incomingFlags =
        opContext.getSearchContext() != null ? opContext.getSearchContext().getSearchFlags() : null;
    final boolean skipAggs =
        incomingFlags != null && Boolean.TRUE.equals(incomingFlags.isSkipAggregates());
    final boolean needFacets = !facets.isEmpty() && !skipAggs;

    // Start facets fetch in parallel when needed (aggregations-only keyword path)
    CompletableFuture<SearchResult> facetsFuture = null;
    if (needFacets) {
      facetsFuture = fetchKeywordFacetsAsync(opContext, entityNames, postFilters, facets);
    }

    // 1) Execute semantic search
    SearchResult semantic =
        semanticEntitySearchService.search(
            opContext,
            entityNames,
            input,
            postFilters,
            (sortCriteria == null || sortCriteria.isEmpty()) ? null : sortCriteria.get(0),
            from,
            size);

    // 2) Preserve kNN order with SimpleRanker
    try {
      semantic =
          semantic
              .copy()
              .setEntities(new SearchEntityArray(new SimpleRanker().rank(semantic.getEntities())));
    } catch (Exception e) {
      log.error("Failed to rank (semantic): {}, exception - {}", semantic, e.toString());
      throw new RuntimeException("Failed to rank " + semantic.toString());
    }

    // 3) Consume parallel facets future if requested
    if (needFacets && facetsFuture != null) {
      try {
        SearchResult facetsOnly = facetsFuture.join();
        if (facetsOnly.getMetadata() != null) {
          semantic.getMetadata().setAggregations(facetsOnly.getMetadata().getAggregations());
        }
      } catch (CompletionException e) {
        Throwable cause = (e.getCause() != null) ? e.getCause() : e;
        throw new RuntimeException("Failed to fetch keyword facets for semantic search", cause);
      }
    }

    // 4) Attach additional aggregates
    semantic
        .getMetadata()
        .setAggregations(
            withAdditionalAggregates(
                semantic.getEntities(), semantic.getMetadata().getAggregations(), facets));

    return semantic;
  }

  /**
   * Kick off a parallel, aggregations-only keyword facet fetch for the given entities and filters.
   * Uses explicit flags to ensure facets are computed without highlights and includes default
   * facets.
   */
  @Nonnull
  private CompletableFuture<SearchResult> fetchKeywordFacetsAsync(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter postFilters,
      @Nonnull List<String> facets) {
    // Clone incoming flags to avoid capturing shared mutable state in the async task.
    // This prevents potential race conditions if the original SearchFlags are mutated
    // by another thread while this task executes.
    final SearchFlags incomingFlags = copySearchFlags(opContext.getSearchContext());

    return CompletableFuture.supplyAsync(
        () ->
            cachingEntitySearchService.search(
                opContext.withSearchFlags(
                    flags -> {
                      flags
                          .setFulltext(true)
                          .setSkipHighlighting(true)
                          .setSkipAggregates(false)
                          .setIncludeDefaultFacets(true);
                      if (incomingFlags != null && incomingFlags.hasMaxAggValues()) {
                        flags.setMaxAggValues(incomingFlags.getMaxAggValues());
                      }
                      return flags;
                    }),
                entityNames,
                "*",
                postFilters,
                Collections.emptyList(),
                0,
                0,
                facets));
  }

  /**
   * Creates a defensive copy of SearchFlags from the given SearchContext to avoid capturing shared
   * mutable state in async operations.
   *
   * @param searchContext the search context to extract flags from, may be null
   * @return a copy of the SearchFlags, or null if the context or flags are null
   */
  @Nullable
  private static SearchFlags copySearchFlags(@Nullable SearchContext searchContext) {
    if (searchContext == null || searchContext.getSearchFlags() == null) {
      return null;
    }
    try {
      return searchContext.getSearchFlags().copy();
    } catch (CloneNotSupportedException e) {
      // Unreachable: DataTemplate extends Cloneable, so clone() never throws.
      // Java requires handling this checked exception from Object.clone() signature.
      throw new IllegalStateException("Failed to clone SearchFlags", e);
    }
  }

  /**
   * If no entities are provided, fallback to the list of non-empty entities
   *
   * @param inputEntities the requested entities
   * @return some entities to search
   */
  private List<String> getEntitiesToSearch(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<String> inputEntities,
      @Nullable Integer size) {
    List<String> lowercaseEntities =
        inputEntities.stream().map(String::toLowerCase).collect(Collectors.toList());

    if (lowercaseEntities.isEmpty()) {
      return opContext.withSpan(
          "getNonEmptyEntities",
          // Apply entity filtering (hook for custom filtering logic)
          // Custom deployments can override filterEntitiesForSearch.
          () -> filterEntitiesForSearch(entityDocCountCache.getNonEmptyEntities(opContext)),
          MetricUtils.DROPWIZARD_NAME,
          MetricUtils.name(this.getClass(), "getNonEmptyEntities"));
    }

    return lowercaseEntities;
  }

  @Nonnull
  private static AggregationMetadataArray withAdditionalAggregates(
      @Nonnull SearchEntityArray entities,
      @Nullable AggregationMetadataArray aggregates,
      @Nonnull List<String> facets) {
    AggregationMetadataArray aggregationMetadata =
        aggregates == null ? new AggregationMetadataArray() : aggregates;

    if (facets.isEmpty() || facets.contains("entity") || facets.contains("_entityType")) {
      Optional<AggregationMetadata> entityTypeAgg =
          aggregationMetadata.stream()
              .filter(aggMeta -> aggMeta.getName().equals(INDEX_VIRTUAL_FIELD))
              .findFirst();
      if (entityTypeAgg.isPresent()) {
        LongMap numResultsPerEntity = entityTypeAgg.get().getAggregations();
        aggregationMetadata.add(
            new AggregationMetadata()
                .setName("entity")
                .setDisplayName("Type")
                .setAggregations(numResultsPerEntity)
                .setFilterValues(
                    new FilterValueArray(
                        entities.stream()
                            .map(SearchEntity::getEntity)
                            .map(Urn::getEntityType)
                            .distinct()
                            .sorted()
                            .map(
                                entityType ->
                                    new FilterValue()
                                        .setValue(entityType)
                                        .setFacetCount(
                                            numResultsPerEntity.get(entityType.toLowerCase())))
                            .collect(Collectors.toList()))));
      }
    }
    return aggregationMetadata;
  }

  @Nonnull
  private static SearchResult getEmptySearchResult(int from, Integer size) {
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(from)
        .setPageSize(size == null ? 0 : size)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }
}
