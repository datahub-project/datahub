package com.linkedin.metadata.search.cache;

import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
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
  private final boolean enableCache;

  public CacheableSearcher<?> getSearcher(@Nonnull String entityName, @Nonnull String input,
      @Nullable Filter postFilters, @Nullable SortCriterion sortCriterion, @Nullable SearchFlags searchFlags) {
    return new CacheableSearcher<>(cacheManager.getCache(ENTITY_SEARCH_SERVICE_CACHE_NAME), batchSize,
        querySize -> entitySearchService.search(entityName, input, postFilters, sortCriterion, querySize.getFrom(),
            querySize.getSize()), querySize -> Quintet.with(entityName, input, postFilters, sortCriterion, querySize),
        searchFlags, enableCache);
  }
}
