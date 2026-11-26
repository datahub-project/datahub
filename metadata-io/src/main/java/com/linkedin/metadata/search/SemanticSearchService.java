package com.linkedin.metadata.search;

import static com.linkedin.metadata.search.utils.QueryUtils.filterEntitiesForSearch;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateEngineSupport;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRuntimes;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateSurfaces;
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

  // Whether the Search V3 engine can serve the semantic search template, once a read decides it
  @Nullable private volatile Boolean templateEngineSupported;

  private final SearchTemplateEngineSupport templateEngineSupport =
      new SearchTemplateEngineSupport(
          "The semantic search surface's template rollout is on, but the Search V3 cluster runs"
              + " OpenSearch {}. The semantic search template needs OpenSearch 3.5 or later, so"
              + " semantic search stays on its own kNN search.",
          "The semantic search surface's template rollout is on, but the Search V3 engine reads as"
              + " {}, so GMS cannot tell whether it can serve the semantic search template, which"
              + " needs OpenSearch 3.5 or later. Semantic search stays on its own kNN search until a"
              + " later read, at most once a minute, succeeds. Reading the OpenSearch version needs"
              + " the cluster:monitor/main permission, and a cluster in compatibility mode reports"
              + " 7.10.2 whatever it runs.",
          this::currentTimeMillis);

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
   * Whether semantic search serves this request through the semantic search template surface, by
   * the rule the template coordinator applies: templates execute, and that surface's rollout, which
   * is off by default, serves this actor. On OpenSearch before 3.5 it does not, whatever the
   * rollout says, and while the OpenSearch version cannot be read it does not either. A caller
   * decides once per request, then calls either the legacy search, {@link
   * #semanticSearchAcrossEntities} or {@link #semanticSearch}, or {@link
   * #semanticSearchAcrossEntitiesByTemplate}.
   */
  public boolean isServedByTemplate(@Nonnull OperationContext opContext) {
    final SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    if (coordinator == null
        || !coordinator.executes(
            opContext.withSearchSurface(SearchTemplateSurfaces.SEMANTIC_SEARCH))) {
      return false;
    }
    final Boolean supported = templateEngineSupported;
    return supported != null ? supported : readTemplateEngineSupport(opContext);
  }

  /**
   * True only when {@link SearchTemplateEngineSupport} reads the engine as supported: Elasticsearch
   * or OpenSearch 3.5 or later. The engine is read on the first request the rollout serves, so the
   * warnings are logged only with the rollout on. An engine it reads as unsupported or cannot read
   * keeps the request on the legacy search, which works on every engine. A decided answer is kept.
   */
  private boolean readTemplateEngineSupport(@Nonnull OperationContext opContext) {
    final SearchTemplateEngineSupport.Support support = templateEngineSupport.read(opContext);
    if (support != SearchTemplateEngineSupport.Support.UNKNOWN) {
      templateEngineSupported = support == SearchTemplateEngineSupport.Support.SUPPORTED;
    }
    return support == SearchTemplateEngineSupport.Support.SUPPORTED;
  }

  @VisibleForTesting
  long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  /**
   * Semantic search across entity types through the template the semantic search surface selects,
   * {@code urn:li:searchTemplate:semantic} as shipped. The vector rung's page, total, and facets
   * stand: no keyword facet request runs beside it, so the facets count only the neighbors that
   * survived. Only a vector rung may serve the page; anything else fails the search.
   *
   * @param facets facets the vector rung aggregates over its surviving hits, plus the legacy
   *     "entity" aggregate
   */
  @Nonnull
  public SearchResult semanticSearchAcrossEntitiesByTemplate(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    if (!Boolean.TRUE.equals(searchServiceConfig.isSemanticSearchEnabled())) {
      throw new SemanticSearchDisabledException();
    }
    size = ConfigUtils.applyLimit(searchServiceConfig, size);
    // The template gates match entity types to registry names, such as glossaryTerm, so this path
    // takes each type's registry name instead of the lowercased one the legacy search uses
    List<String> entitiesToSearch =
        getEntitiesToSearch(opContext, entityNames, size).stream()
            .map(entityName -> opContext.getEntityRegistry().getEntitySpec(entityName).getName())
            .collect(Collectors.toList());
    if (entitiesToSearch.isEmpty()) {
      return getEmptySearchResult(from, size);
    }
    // The surface's slot sits under the fulltext seed. The query feeds the embedding, or names the
    // anchor when it is an entity URN that the semantic template routes to similarity, so semantic
    // search has no structured form here either.
    SearchResult result =
        cachingEntitySearchService.search(
            opContext
                .withSearchSurface(SearchTemplateSurfaces.SEMANTIC_SEARCH)
                .withSearchFlags(flags -> flags.setFulltext(true)),
            entitiesToSearch,
            input,
            postFilters,
            sortCriteria,
            from,
            size,
            facets);
    result
        .getMetadata()
        .setAggregations(
            withAdditionalAggregates(
                result.getEntities(), result.getMetadata().getAggregations(), facets));
    return result;
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
                opContext
                    .withSearchSurface(SearchTemplateSurfaces.AGGREGATE_ENTITIES)
                    .withSearchFlags(
                        flags -> {
                          flags
                              .setFulltext(true)
                              .setSkipHighlighting(true)
                              .setSkipAggregates(false)
                              .setIncludeDefaultFacets(true);
                          if (incomingFlags != null && incomingFlags.hasMaxAggValues()) {
                            flags.setMaxAggValues(incomingFlags.getMaxAggValues());
                          }
                          if (incomingFlags != null && incomingFlags.hasSearchTemplate()) {
                            flags.setSearchTemplate(incomingFlags.getSearchTemplate());
                          }
                          if (incomingFlags != null && incomingFlags.hasIncludeExplain()) {
                            flags.setIncludeExplain(incomingFlags.isIncludeExplain());
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
          () ->
              filterEntitiesForSearch(
                  entityDocCountCache.getNonEmptyEntities(
                      opContext.withSkipTemplateExecution(true))),
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
                                            // V3 keys are registry entity names; V2 keys are
                                            // lower-cased
                                            numResultsPerEntity.getOrDefault(
                                                entityType,
                                                numResultsPerEntity.getOrDefault(
                                                    entityType.toLowerCase(), 0L))))
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
