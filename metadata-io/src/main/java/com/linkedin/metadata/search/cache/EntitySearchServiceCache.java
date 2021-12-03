package com.linkedin.metadata.search.cache;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchOptions;
import com.linkedin.metadata.search.utils.SearchUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.javatuples.Quintet;
import org.springframework.cache.CacheManager;


@RequiredArgsConstructor
public class EntitySearchServiceCache {
  private static final String ENTITY_SEARCH_SERVICE_CACHE_NAME = "entitySearchService";

  private final CacheManager cacheManager;
  private final EntitySearchService entitySearchService;
  private final int batchSize;

  public CacheableSearcher<?> getSearcher(@Nonnull String entityName, @Nonnull String input,
      @Nullable Filter postFilters, @Nullable SortCriterion sortCriterion, @Nullable SearchOptions searchOptions) {
    return new CacheableSearcher<>(cacheManager.getCache(ENTITY_SEARCH_SERVICE_CACHE_NAME), batchSize,
        querySize -> entitySearchService.search(entityName, input, postFilters, sortCriterion, querySize.getFrom(),
            querySize.getSize(), searchOptions),
        querySize -> Quintet.with(entityName, input, postFilters, sortCriterion, querySize),
        SearchUtils.skipCache(searchOptions));
  }
}
