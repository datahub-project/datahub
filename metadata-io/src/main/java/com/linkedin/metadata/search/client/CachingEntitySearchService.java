package com.linkedin.metadata.search.client;

import static com.datahub.util.RecordUtils.toJsonString;
import static com.datahub.util.RecordUtils.toRecordTemplate;

import com.codahale.metrics.Timer;
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
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
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
   * @param entityName the name of the entity to search
   * @param query the search query
   * @param filters the filters to include
   * @param sortCriterion the sort criterion
   * @param from the start offset
   * @param size the count
   * @param flags additional search flags
   * @param facets list of facets we want aggregations for
   * @return a {@link SearchResult} containing the requested batch of search results
   */
  public SearchResult search(
      @Nonnull List<String> entityNames,
      @Nonnull String query,
      @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size,
      @Nullable SearchFlags flags,
      @Nullable List<String> facets) {
    return getCachedSearchResults(
        entityNames, query, filters, sortCriterion, from, size, flags, facets);
  }

  /**
   * Retrieves cached auto complete results
   *
   * @param entityName the name of the entity to search
   * @param input the input query
   * @param filters the filters to include
   * @param limit the max number of results to return
   * @param flags additional search flags
   * @return a {@link SearchResult} containing the requested batch of search results
   */
  public AutoCompleteResult autoComplete(
      @Nonnull String entityName,
      @Nonnull String input,
      @Nullable String field,
      @Nullable Filter filters,
      int limit,
      @Nullable SearchFlags flags) {
    return getCachedAutoCompleteResults(entityName, input, field, filters, limit, flags);
  }

  /**
   * Retrieves cached auto complete results
   *
   * @param entityName type of entity to query
   * @param path the path to be browsed
   * @param filters the request map with fields and values as filters
   * @param from index of the first entity located in path
   * @param size the max number of entities contained in the response
   * @return a {@link SearchResult} containing the requested batch of search results
   */
  public BrowseResult browse(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filters,
      int from,
      int size,
      @Nullable SearchFlags flags) {
    return getCachedBrowseResults(entityName, path, filters, from, size, flags);
  }

  /**
   * Retrieves cached scroll results. If the query has been cached, this will return quickly. If
   * not, a full scroll request will be made.
   *
   * @param entities the names of the entities to search
   * @param query the search query
   * @param filters the filters to include
   * @param sortCriterion the sort criterion
   * @param scrollId opaque scroll identifier for a scroll request
   * @param keepAlive the string representation of how long to keep point in time alive
   * @param size the count
   * @param flags additional search flags
   * @return a {@link ScrollResult} containing the requested batch of scroll results
   */
  public ScrollResult scroll(
      @Nonnull List<String> entities,
      @Nonnull String query,
      @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int size,
      @Nullable SearchFlags flags) {
    return getCachedScrollResults(
        entities, query, filters, sortCriterion, scrollId, keepAlive, size, flags);
  }

  /**
   * Get search results corresponding to the input "from" and "size" It goes through batches,
   * starting from the beginning, until we get enough results to return This lets us have batches
   * that return a variable number of results (we have no idea which batch the "from" "size" page
   * corresponds to)
   */
  public SearchResult getCachedSearchResults(
      @Nonnull List<String> entityNames,
      @Nonnull String query,
      @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion,
      int from,
      int size,
      @Nullable SearchFlags flags,
      @Nullable List<String> facets) {
    return new CacheableSearcher<>(
            cacheManager.getCache(ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME),
            batchSize,
            querySize ->
                getRawSearchResults(
                    entityNames,
                    query,
                    filters,
                    sortCriterion,
                    querySize.getFrom(),
                    querySize.getSize(),
                    flags,
                    facets),
            querySize ->
                Septet.with(
                    entityNames,
                    query,
                    filters != null ? toJsonString(filters) : null,
                    sortCriterion != null ? toJsonString(sortCriterion) : null,
                    flags != null ? toJsonString(flags) : null,
                    facets,
                    querySize),
            flags,
            enableCache)
        .getSearchResults(from, size);
  }

  /** Returns cached auto-complete results. */
  public AutoCompleteResult getCachedAutoCompleteResults(
      @Nonnull String entityName,
      @Nonnull String input,
      @Nullable String field,
      @Nullable Filter filters,
      int limit,
      @Nullable SearchFlags flags) {
    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "getCachedAutoCompleteResults").time()) {
      Cache cache = cacheManager.getCache(ENTITY_SEARCH_SERVICE_AUTOCOMPLETE_CACHE_NAME);
      AutoCompleteResult result;
      if (enableCache(flags)) {
        try (Timer.Context ignored2 =
            MetricUtils.timer(this.getClass(), "getCachedAutoCompleteResults_cache").time()) {
          Timer.Context cacheAccess =
              MetricUtils.timer(this.getClass(), "autocomplete_cache_access").time();
          Object cacheKey =
              Sextet.with(
                  entityName,
                  input,
                  field,
                  filters != null ? toJsonString(filters) : null,
                  flags != null ? toJsonString(flags) : null,
                  limit);
          String json = cache.get(cacheKey, String.class);
          result = json != null ? toRecordTemplate(AutoCompleteResult.class, json) : null;
          cacheAccess.stop();
          if (result == null) {
            Timer.Context cacheMiss =
                MetricUtils.timer(this.getClass(), "autocomplete_cache_miss").time();
            result = getRawAutoCompleteResults(entityName, input, field, filters, limit);
            cache.put(cacheKey, toJsonString(result));
            cacheMiss.stop();
            MetricUtils.counter(this.getClass(), "autocomplete_cache_miss_count").inc();
          }
        }
      } else {
        result = getRawAutoCompleteResults(entityName, input, field, filters, limit);
      }
      return result;
    }
  }

  /** Returns cached browse results. */
  public BrowseResult getCachedBrowseResults(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filters,
      int from,
      int size,
      @Nullable SearchFlags flags) {
    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "getCachedBrowseResults").time()) {
      Cache cache = cacheManager.getCache(ENTITY_SEARCH_SERVICE_BROWSE_CACHE_NAME);
      BrowseResult result;
      if (enableCache(flags)) {
        try (Timer.Context ignored2 =
            MetricUtils.timer(this.getClass(), "getCachedBrowseResults_cache").time()) {
          Timer.Context cacheAccess =
              MetricUtils.timer(this.getClass(), "browse_cache_access").time();
          Object cacheKey =
              Sextet.with(
                  entityName,
                  path,
                  filters != null ? toJsonString(filters) : null,
                  flags != null ? toJsonString(flags) : null,
                  from,
                  size);
          String json = cache.get(cacheKey, String.class);
          result = json != null ? toRecordTemplate(BrowseResult.class, json) : null;
          cacheAccess.stop();
          if (result == null) {
            Timer.Context cacheMiss =
                MetricUtils.timer(this.getClass(), "browse_cache_miss").time();
            result = getRawBrowseResults(entityName, path, filters, from, size);
            cache.put(cacheKey, toJsonString(result));
            cacheMiss.stop();
            MetricUtils.counter(this.getClass(), "browse_cache_miss_count").inc();
          }
        }
      } else {
        result = getRawBrowseResults(entityName, path, filters, from, size);
      }
      return result;
    }
  }

  /** Returns cached scroll results. */
  public ScrollResult getCachedScrollResults(
      @Nonnull List<String> entities,
      @Nonnull String query,
      @Nullable Filter filters,
      @Nullable SortCriterion sortCriterion,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int size,
      @Nullable SearchFlags flags) {
    try (Timer.Context ignored =
        MetricUtils.timer(this.getClass(), "getCachedScrollResults").time()) {
      boolean isFullText =
          Boolean.TRUE.equals(Optional.ofNullable(flags).orElse(new SearchFlags()).isFulltext());
      Cache cache = cacheManager.getCache(ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME);
      ScrollResult result;
      if (enableCache(flags)) {
        Timer.Context cacheAccess =
            MetricUtils.timer(this.getClass(), "scroll_cache_access").time();
        Object cacheKey =
            Septet.with(
                entities,
                query,
                filters != null ? toJsonString(filters) : null,
                sortCriterion != null ? toJsonString(sortCriterion) : null,
                flags != null ? toJsonString(flags) : null,
                scrollId,
                size);
        String json = cache.get(cacheKey, String.class);
        result = json != null ? toRecordTemplate(ScrollResult.class, json) : null;
        cacheAccess.stop();
        if (result == null) {
          Timer.Context cacheMiss = MetricUtils.timer(this.getClass(), "scroll_cache_miss").time();
          result =
              getRawScrollResults(
                  entities,
                  query,
                  filters,
                  sortCriterion,
                  scrollId,
                  keepAlive,
                  size,
                  isFullText,
                  flags);
          cache.put(cacheKey, toJsonString(result));
          cacheMiss.stop();
          MetricUtils.counter(this.getClass(), "scroll_cache_miss_count").inc();
        }
      } else {
        result =
            getRawScrollResults(
                entities,
                query,
                filters,
                sortCriterion,
                scrollId,
                keepAlive,
                size,
                isFullText,
                flags);
      }
      return result;
    }
  }

  /** Executes the expensive search query using the {@link EntitySearchService} */
  private SearchResult getRawSearchResults(
      final List<String> entityNames,
      final String input,
      final Filter filters,
      final SortCriterion sortCriterion,
      final int start,
      final int count,
      @Nullable final SearchFlags searchFlags,
      @Nullable final List<String> facets) {
    return entitySearchService.search(
        entityNames, input, filters, sortCriterion, start, count, searchFlags, facets);
  }

  /** Executes the expensive autocomplete query using the {@link EntitySearchService} */
  private AutoCompleteResult getRawAutoCompleteResults(
      final String entityName,
      final String input,
      final String field,
      final Filter filters,
      final int limit) {
    return entitySearchService.autoComplete(entityName, input, field, filters, limit);
  }

  /** Executes the expensive autocomplete query using the {@link EntitySearchService} */
  private BrowseResult getRawBrowseResults(
      final String entityName,
      final String input,
      final Filter filters,
      final int start,
      final int count) {
    return entitySearchService.browse(entityName, input, filters, start, count);
  }

  /** Executes the expensive search query using the {@link EntitySearchService} */
  private ScrollResult getRawScrollResults(
      final List<String> entities,
      final String input,
      final Filter filters,
      final SortCriterion sortCriterion,
      @Nullable final String scrollId,
      @Nullable final String keepAlive,
      final int count,
      final boolean fulltext,
      @Nullable final SearchFlags searchFlags) {
    if (fulltext) {
      return entitySearchService.fullTextScroll(
          entities, input, filters, sortCriterion, scrollId, keepAlive, count, searchFlags);
    } else {
      return entitySearchService.structuredScroll(
          entities, input, filters, sortCriterion, scrollId, keepAlive, count, searchFlags);
    }
  }

  /** Returns true if the cache should be used or skipped when fetching search results */
  private boolean enableCache(final SearchFlags searchFlags) {
    return enableCache && (searchFlags == null || !searchFlags.isSkipCache());
  }
}
