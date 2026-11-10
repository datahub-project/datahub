package com.linkedin.metadata.search.client;

import static com.datahub.util.RecordUtils.toJsonString;
import static com.datahub.util.RecordUtils.toRecordTemplate;
import static com.linkedin.metadata.utils.metrics.MetricUtils.CACHE_HIT_ATTR;

import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.cache.CacheableSearcher;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateHybrid;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRuntimes;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.opentelemetry.api.trace.Span;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.javatuples.Octet;
import org.javatuples.Septet;
import org.javatuples.Sextet;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

@Slf4j
@RequiredArgsConstructor
public class CachingEntitySearchService {
  public static final String ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME = "entitySearchServiceSearch";
  public static final String ENTITY_SEARCH_SERVICE_AUTOCOMPLETE_CACHE_NAME =
      "entitySearchServiceAutoComplete";
  public static final String ENTITY_SEARCH_SERVICE_BROWSE_CACHE_NAME = "entitySearchServiceBrowse";
  public static final String ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME = "entitySearchServiceScroll";

  private final CacheManager cacheManager;
  private final EntitySearchService
      entitySearchService; // This is a shared component, also used in search aggregation
  private final int batchSize;
  private final boolean enableCache;

  /** Optional. When set, a template kNN section blends document hits into keyword search. */
  @Nullable private SemanticEntitySearch semanticEntitySearch;

  public void setSemanticEntitySearch(@Nullable SemanticEntitySearch semanticEntitySearch) {
    this.semanticEntitySearch = semanticEntitySearch;
  }

  /**
   * Retrieves cached search results. If the query has been cached, this will return quickly. If
   * not, a full search request will be made.
   *
   * @param opContext the operation's context
   * @param entityNames the names of the entity to search
   * @param query the search query
   * @param filters the filters to include
   * @param sortCriteria the sort criteria
   * @param from the start offset
   * @param size the count
   * @param facets list of facets we want aggregations for
   * @return a {@link SearchResult} containing the requested batch of search results
   */
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String query,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    return getCachedSearchResults(
        opContext, entityNames, query, filters, sortCriteria, from, size, facets);
  }

  /**
   * Retrieves cached auto complete results
   *
   * @param opContext the operation's context
   * @param entityName the name of the entity to search
   * @param input the input query
   * @param filters the filters to include
   * @param limit the max number of results to return
   * @return a {@link SearchResult} containing the requested batch of search results
   */
  public AutoCompleteResult autoComplete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String input,
      @Nullable String field,
      @Nullable Filter filters,
      @Nullable Integer limit) {
    return getCachedAutoCompleteResults(opContext, entityName, input, field, filters, limit);
  }

  /**
   * Retrieves cached auto complete results
   *
   * @param opContext the operation's context
   * @param entityName type of entity to query
   * @param path the path to be browsed
   * @param filters the request map with fields and values as filters
   * @param from index of the first entity located in path
   * @param size the max number of entities contained in the response
   * @return a {@link SearchResult} containing the requested batch of search results
   */
  public BrowseResult browse(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filters,
      int from,
      @Nullable Integer size) {
    return getCachedBrowseResults(opContext, entityName, path, filters, from, size);
  }

  /**
   * Retrieves cached scroll results. If the query has been cached, this will return quickly. If
   * not, a full scroll request will be made.
   *
   * @param opContext the operation's context
   * @param entities the names of the entities to search
   * @param query the search query
   * @param filters the filters to include
   * @param sortCriteria the sort criteria
   * @param scrollId opaque scroll identifier for a scroll request
   * @param keepAlive the string representation of how long to keep point in time alive
   * @param size the count
   * @return a {@link ScrollResult} containing the requested batch of scroll results
   */
  public ScrollResult scroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String query,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    return getCachedScrollResults(
        opContext, entities, query, filters, sortCriteria, scrollId, keepAlive, size, facets);
  }

  /**
   * Get search results corresponding to the input "from" and "size" It goes through batches,
   * starting from the beginning, until we get enough results to return This lets us have batches
   * that return a variable number of results (we have no idea which batch the "from" "size" page
   * corresponds to)
   */
  public SearchResult getCachedSearchResults(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String query,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    size = ConfigUtils.applyLimit(entitySearchService.getSearchServiceConfig(), size);
    OperationContext bound = SearchTemplateRuntimes.bind(opContext, query, false, entityNames);
    return new CacheableSearcher<>(
            cacheManager.getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME),
            batchSize,
            querySize ->
                getRawSearchResults(
                    bound,
                    entityNames,
                    query,
                    filters,
                    sortCriteria,
                    querySize.getFrom(),
                    querySize.getSize(),
                    facets),
            querySize ->
                Septet.with(
                    bound.getSearchContextId(),
                    cacheKeyList(entityNames),
                    query,
                    filters != null ? toJsonString(filters) : null,
                    CollectionUtils.isNotEmpty(sortCriteria) ? toJsonString(sortCriteria) : null,
                    cacheKeyList(facets),
                    querySize),
            enableCache)
        .getSearchResults(opContext, from, size);
  }

  /** Returns cached auto-complete results. */
  public AutoCompleteResult getCachedAutoCompleteResults(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String input,
      @Nullable String field,
      @Nullable Filter filters,
      @Nullable Integer limit) {

    return opContext.withSpan(
        "getAutoCompleteResults",
        () -> {
          Cache cache = cacheManager.getCache(ENTITY_SEARCH_SERVICE_AUTOCOMPLETE_CACHE_NAME);
          AutoCompleteResult result;
          if (enableCache(opContext.getSearchContext().getSearchFlags())) {

            Object cacheKey =
                Sextet.with(
                    opContext.getSearchContextId(),
                    entityName,
                    input,
                    field,
                    filters != null ? toJsonString(filters) : null,
                    limit);
            String json = cache.get(cacheKey, String.class);
            result = json != null ? toRecordTemplate(AutoCompleteResult.class, json) : null;

            if (result == null) {
              result =
                  getRawAutoCompleteResults(opContext, entityName, input, field, filters, limit);
              cache.put(cacheKey, toJsonString(result));
              Span.current().setAttribute(CACHE_HIT_ATTR, false);
              opContext
                  .getMetricUtils()
                  .ifPresent(
                      metricUtils ->
                          metricUtils.increment(
                              this.getClass(), "autocomplete_cache_miss_count", 1));
            } else {
              Span.current().setAttribute(CACHE_HIT_ATTR, true);
            }
          } else {
            Span.current().setAttribute(CACHE_HIT_ATTR, false);
            result = getRawAutoCompleteResults(opContext, entityName, input, field, filters, limit);
          }
          return result;
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getCachedAutoCompleteResults"));
  }

  /** Returns cached browse results. */
  public BrowseResult getCachedBrowseResults(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filters,
      int from,
      @Nullable Integer size) {

    return opContext.withSpan(
        "getBrowseResults",
        () -> {
          Cache cache = cacheManager.getCache(ENTITY_SEARCH_SERVICE_BROWSE_CACHE_NAME);
          BrowseResult result;
          if (enableCache(opContext.getSearchContext().getSearchFlags())) {
            Object cacheKey =
                Sextet.with(
                    opContext.getSearchContextId(),
                    entityName,
                    path,
                    filters != null ? toJsonString(filters) : null,
                    from,
                    size);
            String json = cache.get(cacheKey, String.class);
            result = json != null ? toRecordTemplate(BrowseResult.class, json) : null;

            if (result == null) {
              result = getRawBrowseResults(opContext, entityName, path, filters, from, size);
              cache.put(cacheKey, toJsonString(result));
              Span.current().setAttribute(CACHE_HIT_ATTR, false);
              opContext
                  .getMetricUtils()
                  .ifPresent(
                      metricUtils ->
                          metricUtils.increment(this.getClass(), "browse_cache_miss_count", 1));
            } else {
              Span.current().setAttribute(CACHE_HIT_ATTR, true);
            }
          } else {
            Span.current().setAttribute(CACHE_HIT_ATTR, false);
            result = getRawBrowseResults(opContext, entityName, path, filters, from, size);
          }
          return result;
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getCachedBrowseResults"));
  }

  /** Returns cached scroll results. */
  public ScrollResult getCachedScrollResults(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String query,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {

    return opContext.withSpan(
        "getScrollResults",
        () -> {
          OperationContext bound = SearchTemplateRuntimes.bind(opContext, query, true, entities);
          boolean isFullText =
              Boolean.TRUE.equals(
                  Optional.ofNullable(bound.getSearchContext().getSearchFlags())
                      .orElse(new SearchFlags())
                      .isFulltext());
          Cache cache = cacheManager.getCache(ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME);
          ScrollResult result;
          if (enableCache(bound.getSearchContext().getSearchFlags())) {

            Object cacheKey =
                Octet.with(
                    bound.getSearchContextId(),
                    cacheKeyList(entities),
                    query,
                    filters != null ? toJsonString(filters) : null,
                    CollectionUtils.isNotEmpty(sortCriteria) ? toJsonString(sortCriteria) : null,
                    scrollId,
                    cacheKeyList(facets),
                    size);
            String json = cache.get(cacheKey, String.class);
            result = json != null ? toRecordTemplate(ScrollResult.class, json) : null;

            if (result == null) {
              result =
                  getRawScrollResults(
                      bound,
                      entities,
                      query,
                      filters,
                      sortCriteria,
                      scrollId,
                      keepAlive,
                      size,
                      isFullText,
                      facets);
              cache.put(cacheKey, toJsonString(result));
              Span.current().setAttribute(CACHE_HIT_ATTR, false);
              opContext
                  .getMetricUtils()
                  .ifPresent(
                      metricUtils ->
                          metricUtils.increment(this.getClass(), "scroll_cache_miss_count", 1));
            } else {
              Span.current().setAttribute(CACHE_HIT_ATTR, true);
            }
          } else {
            Span.current().setAttribute(CACHE_HIT_ATTR, false);
            result =
                getRawScrollResults(
                    bound,
                    entities,
                    query,
                    filters,
                    sortCriteria,
                    scrollId,
                    keepAlive,
                    size,
                    isFullText,
                    facets);
          }
          return result;
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getCachedScrollResults"));
  }

  /** Executes the expensive search query using the {@link EntitySearchService} */
  private SearchResult getRawSearchResults(
      @Nonnull OperationContext opContext,
      final List<String> entityNames,
      final String input,
      final Filter filters,
      final List<SortCriterion> sortCriteria,
      final int start,
      @Nullable Integer count,
      @Nonnull final List<String> facets) {
    SearchResult keyword =
        entitySearchService.search(
            opContext, entityNames, input, filters, sortCriteria, start, count, facets);
    return rerankDocuments(opContext, entityNames, input, filters, sortCriteria, start, keyword);
  }

  /**
   * Reorders document rows inside the first keyword window. Later pages, a non-score sort, and a
   * window with no documents keep keyword order. The total and the facets stay the keyword query's.
   */
  @Nonnull
  private SearchResult rerankDocuments(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter filters,
      @Nullable List<SortCriterion> sortCriteria,
      int start,
      @Nonnull SearchResult keyword) {
    ResolvedSearchTemplate resolved = opContext.getSearchContext().getResolvedSearchTemplate();
    if (semanticEntitySearch == null
        || resolved == null
        || !resolved.isKnnApplied()
        || !SearchTemplateHybrid.includesDocument(entityNames)
        || start > 0
        || !scoreSort(sortCriteria)
        || !SearchTemplateHybrid.hasDocument(keyword)) {
      return keyword;
    }
    List<String> documentUrns = documentUrns(keyword);
    if (documentUrns.isEmpty()) {
      return keyword;
    }
    Float minScore = null;
    SearchFlags flags = opContext.getSearchContext().getSearchFlags();
    if (flags != null && flags.hasMinScore() && flags.getMinScore() != null) {
      minScore = flags.getMinScore();
    }
    SearchTemplateCoordinator coordinator = SearchTemplateRuntimes.coordinator(opContext);
    int rerankTimeoutSeconds = coordinator == null ? 2 : coordinator.documentRerankTimeoutSeconds();
    CompletableFuture<SearchResult> rerank =
        CompletableFuture.supplyAsync(
            () ->
                semanticEntitySearch.search(
                    opContext,
                    List.of(SearchTemplateHybrid.DOCUMENT),
                    input,
                    documentFilter(filters, documentUrns),
                    null,
                    0,
                    documentUrns.size()));
    try {
      SearchResult knn = rerank.get(rerankTimeoutSeconds, TimeUnit.SECONDS);
      if (coordinator == null) {
        return keyword;
      }
      return SearchTemplateHybrid.rerank(
          keyword,
          knn,
          resolved.getKeywordWeight(),
          resolved.getVectorWeight(),
          minScore,
          coordinator.hybridKeywordScale());
    } catch (TimeoutException e) {
      rerank.cancel(true);
      log.warn("Document rerank timed out; returning keyword order");
      return keyword;
    } catch (ExecutionException e) {
      rerank.cancel(true);
      log.warn("Document rerank failed; returning keyword order", e.getCause());
      return keyword;
    } catch (InterruptedException e) {
      rerank.cancel(true);
      Thread.currentThread().interrupt();
      log.warn("Document rerank interrupted; returning keyword order");
      return keyword;
    } catch (RuntimeException e) {
      rerank.cancel(true);
      log.warn("Document rerank failed; returning keyword order", e);
      return keyword;
    }
  }

  @Nonnull
  private static List<String> documentUrns(@Nonnull SearchResult keyword) {
    List<String> urns = new ArrayList<>();
    if (keyword.getEntities() == null) {
      return urns;
    }
    for (SearchEntity entity : keyword.getEntities()) {
      if (entity.getEntity() != null
          && SearchTemplateHybrid.DOCUMENT.equals(entity.getEntity().getEntityType())) {
        urns.add(entity.getEntity().toString());
      }
    }
    return urns;
  }

  /** The vector candidates are the document rows already in the keyword window. */
  @Nonnull
  private static Filter documentFilter(@Nullable Filter filters, @Nonnull List<String> urns) {
    Criterion urnCriterion = CriterionUtils.buildCriterion("urn", Condition.EQUAL, urns);
    ConjunctiveCriterionArray or = new ConjunctiveCriterionArray();
    if (filters == null || filters.getOr() == null || filters.getOr().isEmpty()) {
      or.add(new ConjunctiveCriterion().setAnd(new CriterionArray(urnCriterion)));
      return new Filter().setOr(or);
    }
    for (ConjunctiveCriterion conjunction : filters.getOr()) {
      CriterionArray and = new CriterionArray();
      if (conjunction.getAnd() != null) {
        and.addAll(conjunction.getAnd());
      }
      and.add(urnCriterion);
      or.add(new ConjunctiveCriterion().setAnd(and));
    }
    return new Filter().setOr(or);
  }

  private static boolean scoreSort(@Nullable List<SortCriterion> sortCriteria) {
    if (sortCriteria == null || sortCriteria.isEmpty()) {
      return true;
    }
    for (SortCriterion criterion : sortCriteria) {
      String field = criterion.getField();
      if (field != null && !"_score".equals(field) && !"score".equals(field)) {
        return false;
      }
      if (criterion.getOrder() == SortOrder.ASCENDING) {
        return false;
      }
    }
    return true;
  }

  /** Executes the expensive autocomplete query using the {@link EntitySearchService} */
  private AutoCompleteResult getRawAutoCompleteResults(
      @Nonnull OperationContext opContext,
      final String entityName,
      final String input,
      final String field,
      final Filter filters,
      @Nullable Integer limit) {
    return entitySearchService.autoComplete(opContext, entityName, input, field, filters, limit);
  }

  /** Executes the expensive autocomplete query using the {@link EntitySearchService} */
  private BrowseResult getRawBrowseResults(
      @Nonnull OperationContext opContext,
      final String entityName,
      final String input,
      final Filter filters,
      final int start,
      @Nullable Integer limit) {
    return entitySearchService.browse(opContext, entityName, input, filters, start, limit);
  }

  /** Executes the expensive search query using the {@link EntitySearchService} */
  private ScrollResult getRawScrollResults(
      @Nonnull OperationContext opContext,
      final List<String> entities,
      final String input,
      final Filter filters,
      final List<SortCriterion> sortCriteria,
      @Nullable final String scrollId,
      @Nullable final String keepAlive,
      @Nullable Integer count,
      final boolean fulltext,
      @Nonnull List<String> facets) {
    if (fulltext) {
      return entitySearchService.fullTextScroll(
          opContext, entities, input, filters, sortCriteria, scrollId, keepAlive, count, facets);
    } else {
      return entitySearchService.structuredScroll(
          opContext, entities, input, filters, sortCriteria, scrollId, keepAlive, count, facets);
    }
  }

  /**
   * Cache keys are Java-serialized into Hazelcast and read back by peers (e.g. key scans during
   * eviction). The embedded member only accepts JDK and DataHub types, so callers' list types
   * (Guava {@code ImmutableList} and similar) are copied into an {@link ArrayList}.
   */
  @Nullable
  private static List<String> cacheKeyList(@Nullable List<String> values) {
    return values == null ? null : new ArrayList<>(values);
  }

  /** Returns true if the cache should be used or skipped when fetching search results */
  private boolean enableCache(@Nullable final SearchFlags searchFlags) {
    return enableCache && (searchFlags == null || !searchFlags.isSkipCache());
  }
}
