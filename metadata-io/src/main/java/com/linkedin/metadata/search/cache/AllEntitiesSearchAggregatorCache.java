package com.linkedin.metadata.search.cache;

import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.aggregator.AllEntitiesSearchAggregator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.javatuples.Quintet;
import org.springframework.cache.CacheManager;


@RequiredArgsConstructor
public class AllEntitiesSearchAggregatorCache {
  private static final String ALL_ENTITIES_SEARCH_AGGREGATOR_CACHE_NAME = "allEntitiesSearchAggregator";

  private final CacheManager cacheManager;
  private final AllEntitiesSearchAggregator aggregator;
  private final int batchSize;
  private final boolean enableCache;

  public CacheableSearcher<?> getSearcher(List<String> entities, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, @Nullable SearchFlags searchFlags) {
    return new CacheableSearcher<>(cacheManager.getCache(ALL_ENTITIES_SEARCH_AGGREGATOR_CACHE_NAME), batchSize,
        querySize -> aggregator.search(entities, input, postFilters, sortCriterion, querySize.getFrom(),
            querySize.getSize(), searchFlags),
        querySize -> Quintet.with(entities, input, postFilters, sortCriterion, querySize), searchFlags, enableCache);
  }
}
