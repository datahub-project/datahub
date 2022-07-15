package com.linkedin.metadata.search;

import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.cache.CachingAllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.ranker.SearchRanker;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SearchService {
  private final CachingEntitySearchService _cachingEntitySearchService;
  private final CachingAllEntitiesSearchAggregator _cachingAllEntitiesSearchAggregator;
  private final EntityDocCountCache _entityDocCountCache;
  private final SearchRanker _searchRanker;

  public SearchService(
      EntityDocCountCache entityDocCountCache,
      CachingEntitySearchService cachingEntitySearchService,
      CachingAllEntitiesSearchAggregator cachingEntitySearchAggregator,
      SearchRanker searchRanker) {
    _cachingEntitySearchService = cachingEntitySearchService;
    _cachingAllEntitiesSearchAggregator = cachingEntitySearchAggregator;
    _searchRanker = searchRanker;
    _entityDocCountCache = entityDocCountCache;
  }

  public Map<String, Long> docCountPerEntity(@Nonnull List<String> entityNames) {
    return entityNames.stream()
        .collect(Collectors.toMap(Function.identity(),
            entityName -> _entityDocCountCache.getEntityDocCount().getOrDefault(entityName.toLowerCase(), 0L)));
  }

  /**
   * Gets a list of documents that match given search request. The results are aggregated and filters are applied to the
   * search hits and not the aggregation results.
   *
   * @param entityName name of the entity
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param searchFlags optional set of flags to control search behavior
   * @return a {@link com.linkedin.metadata.dao.SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public SearchResult search(@Nonnull String entityName, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size, @Nullable SearchFlags searchFlags) {
    SearchResult result =
        _cachingEntitySearchService.search(entityName, input, postFilters, sortCriterion, from, size, searchFlags);

    try {
      return result.copy().setEntities(new SearchEntityArray(_searchRanker.rank(result.getEntities())));
    } catch (Exception e) {
      log.error("Failed to rank: {}, execption - {}", result, e.toString());
      throw new RuntimeException("Failed to rank " + result.toString());
    }
  }

  /**
   * Gets a list of documents that match given search request across multiple entities. The results are aggregated and filters are applied to the
   * search hits and not the aggregation results.
   *
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param postFilters the request map with fields and values as filters to be applied to search hits
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param searchFlags optional set of flags to control search behavior
   * @return a {@link com.linkedin.metadata.dao.SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public SearchResult searchAcrossEntities(@Nonnull List<String> entities, @Nonnull String input,
      @Nullable Filter postFilters, @Nullable SortCriterion sortCriterion, int from, int size,
      @Nullable SearchFlags searchFlags) {
    log.debug(String.format(
        "Searching Search documents entities: %s, input: %s, postFilters: %s, sortCriterion: %s, from: %s, size: %s",
        entities, input, postFilters, sortCriterion, from, size));
    return _cachingAllEntitiesSearchAggregator.getSearchResults(entities, input, postFilters, sortCriterion, from, size, searchFlags);
  }
}
