package com.linkedin.metadata.search;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.aggregator.AllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.AllEntitiesSearchAggregatorCache;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.cache.EntitySearchServiceCache;
import com.linkedin.metadata.search.ranker.SearchRanker;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;


@Slf4j
public class SearchService {
  private final EntitySearchService _entitySearchService;
  private final AllEntitiesSearchAggregator _aggregator;
  private final SearchRanker _searchRanker;

  private final EntityDocCountCache _entityDocCountCache;
  private final EntitySearchServiceCache _entitySearchServiceCache;
  private final AllEntitiesSearchAggregatorCache _allEntitiesSearchAggregatorCache;

  public SearchService(EntityRegistry entityRegistry, EntitySearchService entitySearchService,
      SearchRanker searchRanker, CacheManager cacheManager, int batchSize, boolean enableCache) {
    _entitySearchService = entitySearchService;
    _searchRanker = searchRanker;
    _aggregator =
        new AllEntitiesSearchAggregator(entityRegistry, entitySearchService, searchRanker, cacheManager, batchSize,
            enableCache);
    _entityDocCountCache = new EntityDocCountCache(entityRegistry, entitySearchService);
    _entitySearchServiceCache = new EntitySearchServiceCache(cacheManager, entitySearchService, batchSize, enableCache);
    _allEntitiesSearchAggregatorCache =
        new AllEntitiesSearchAggregatorCache(cacheManager, _aggregator, batchSize, enableCache);
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
        _entitySearchServiceCache.getSearcher(entityName, input, postFilters, sortCriterion, searchFlags)
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
    return _allEntitiesSearchAggregatorCache.getSearcher(entities, input, postFilters, sortCriterion, searchFlags)
        .getSearchResults(from, size);
  }
}
