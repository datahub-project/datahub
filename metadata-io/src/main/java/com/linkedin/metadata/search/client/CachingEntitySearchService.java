package com.linkedin.metadata.search.client;

import static com.datahub.util.RecordUtils.toJsonString;
import static com.datahub.util.RecordUtils.toRecordTemplate;
import static com.linkedin.metadata.utils.metrics.MetricUtils.CACHE_HIT_ATTR;

import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.cache.CacheableSearcher;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.javatuples.Septet;
import org.javatuples.Sextet;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

@RequiredArgsConstructor
public class CachingEntitySearchService {
  private static final String ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME = "entitySearchServiceSearch";
  private static final String ENTITY_SEARCH_SERVICE_AUTOCOMPLETE_CACHE_NAME =
      "entitySearchServiceAutoComplete";
  private static final String ENTITY_SEARCH_SERVICE_BROWSE_CACHE_NAME = "entitySearchServiceBrowse";
  public static final String ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME = "entitySearchServiceScroll";

  private final CacheManager cacheManager;
  private final EntitySearchService
      entitySearchService; // This is a shared component, also used in search aggregation
  private final int batchSize;
  private final boolean enableCache;

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
      int size,
      @Nullable List<String> facets) {
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
      int limit) {
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
      int size) {
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
      int size) {
    return getCachedScrollResults(
        opContext, entities, query, filters, sortCriteria, scrollId, keepAlive, size);
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
      int size,
      @Nullable List<String> facets) {
    return new CacheableSearcher<>(
            cacheManager.getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME),
            batchSize,
            querySize ->
                getRawSearchResults(
                    opContext,
                    entityNames,
                    query,
                    filters,
                    sortCriteria,
                    querySize.getFrom(),
                    querySize.getSize(),
                    facets),
            querySize ->
                Septet.with(
                    opContext.getSearchContextId(),
                    entityNames,
                    query,
                    filters != null ? toJsonString(filters) : null,
                    CollectionUtils.isNotEmpty(sortCriteria) ? toJsonString(sortCriteria) : null,
                    facets,
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
      int limit) {

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
              MetricUtils.counter(this.getClass(), "autocomplete_cache_miss_count").inc();
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
      int size) {

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
              MetricUtils.counter(this.getClass(), "browse_cache_miss_count").inc();
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
      int size) {

    return opContext.withSpan(
        "getScrollResults",
        () -> {
          boolean isFullText =
              Boolean.TRUE.equals(
                  Optional.ofNullable(opContext.getSearchContext().getSearchFlags())
                      .orElse(new SearchFlags())
                      .isFulltext());
          Cache cache = cacheManager.getCache(ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME);
          ScrollResult result;
          if (enableCache(opContext.getSearchContext().getSearchFlags())) {

            Object cacheKey =
                Septet.with(
                    opContext.getSearchContextId(),
                    entities,
                    query,
                    filters != null ? toJsonString(filters) : null,
                    CollectionUtils.isNotEmpty(sortCriteria) ? toJsonString(sortCriteria) : null,
                    scrollId,
                    size);
            String json = cache.get(cacheKey, String.class);
            result = json != null ? toRecordTemplate(ScrollResult.class, json) : null;

            if (result == null) {
              result =
                  getRawScrollResults(
                      opContext,
                      entities,
                      query,
                      filters,
                      sortCriteria,
                      scrollId,
                      keepAlive,
                      size,
                      isFullText);
              cache.put(cacheKey, toJsonString(result));
              Span.current().setAttribute(CACHE_HIT_ATTR, false);
              MetricUtils.counter(this.getClass(), "scroll_cache_miss_count").inc();
            } else {
              Span.current().setAttribute(CACHE_HIT_ATTR, true);
            }
          } else {
            Span.current().setAttribute(CACHE_HIT_ATTR, false);
            result =
                getRawScrollResults(
                    opContext,
                    entities,
                    query,
                    filters,
                    sortCriteria,
                    scrollId,
                    keepAlive,
                    size,
                    isFullText);
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
      final int count,
      @Nullable final List<String> facets) {
    return entitySearchService.search(
        opContext, entityNames, input, filters, sortCriteria, start, count, facets);
  }

  /** Executes the expensive autocomplete query using the {@link EntitySearchService} */
  private AutoCompleteResult getRawAutoCompleteResults(
      @Nonnull OperationContext opContext,
      final String entityName,
      final String input,
      final String field,
      final Filter filters,
      final int limit) {
    return entitySearchService.autoComplete(opContext, entityName, input, field, filters, limit);
  }

  /** Executes the expensive autocomplete query using the {@link EntitySearchService} */
  private BrowseResult getRawBrowseResults(
      @Nonnull OperationContext opContext,
      final String entityName,
      final String input,
      final Filter filters,
      final int start,
      final int count) {
    return entitySearchService.browse(opContext, entityName, input, filters, start, count);
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
      final int count,
      final boolean fulltext) {
    if (fulltext) {
      return entitySearchService.fullTextScroll(
          opContext, entities, input, filters, sortCriteria, scrollId, keepAlive, count);
    } else {
      return entitySearchService.structuredScroll(
          opContext, entities, input, filters, sortCriteria, scrollId, keepAlive, count);
    }
  }

  /** Returns true if the cache should be used or skipped when fetching search results */
  private boolean enableCache(@Nullable final SearchFlags searchFlags) {
    return enableCache && (searchFlags == null || !searchFlags.isSkipCache());
  }
}
