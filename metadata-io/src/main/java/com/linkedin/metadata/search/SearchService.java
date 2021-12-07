package com.linkedin.metadata.search;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.aggregator.AllEntitiesSearchAggregator;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.cache.AllEntitiesSearchAggregatorCache;
import com.linkedin.metadata.search.cache.EntitySearchServiceCache;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;

@Slf4j
public class SearchService {
  private final EntitySearchService _entitySearchService;
  private final AllEntitiesSearchAggregator _aggregator;
  private final SearchRanker _searchRanker;

  private final EntitySearchServiceCache _entitySearchServiceCache;
  private final AllEntitiesSearchAggregatorCache _allEntitiesSearchAggregatorCache;

  public SearchService(EntityRegistry entityRegistry, EntitySearchService entitySearchService,
      SearchRanker searchRanker, CacheManager cacheManager, int batchSize) {
    _entitySearchService = entitySearchService;
    _searchRanker = searchRanker;
    _aggregator =
        new AllEntitiesSearchAggregator(entityRegistry, entitySearchService, searchRanker, cacheManager, batchSize);
    _entitySearchServiceCache = new EntitySearchServiceCache(cacheManager, entitySearchService, batchSize);
    _allEntitiesSearchAggregatorCache = new AllEntitiesSearchAggregatorCache(cacheManager, _aggregator, batchSize);
  }

  /**
   * Get the number of documents corresponding to the entity
   *
   * @param entityName name of the entity
   */
  public long docCount(@Nonnull String entityName) {
    return _entitySearchService.docCount(entityName);
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
   * @return a {@link com.linkedin.metadata.dao.SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public SearchResult search(@Nonnull String entityName, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    SearchResult result = _entitySearchServiceCache.getSearcher(entityName, input, postFilters, sortCriterion)
        .getSearchResults(from, size);
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
   * @return a {@link com.linkedin.metadata.dao.SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public SearchResult searchAcrossEntities(@Nonnull List<String> entities, @Nonnull String input,
      @Nullable Filter postFilters, @Nullable SortCriterion sortCriterion, int from, int size) {
    log.debug(String.format(
        "Searching Search documents entities: %s, input: %s, postFilters: %s, sortCriterion: %s, from: %s, size: %s",
        entities, input, postFilters, sortCriterion, from, size));
    return _allEntitiesSearchAggregatorCache.getSearcher(entities, input, postFilters, sortCriterion)
        .getSearchResults(from, size);
  }
}
