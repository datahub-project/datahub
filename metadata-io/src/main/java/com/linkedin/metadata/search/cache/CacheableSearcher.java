package com.linkedin.metadata.search.cache;

import com.codahale.metrics.Timer;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.springframework.cache.Cache;


/**
 * Wrapper class to allow searching in batches and caching the results.
 */
@RequiredArgsConstructor
public class CacheableSearcher<K> {
  @Nonnull
  private final Cache cache;
  private final int batchSize;
  // Function that executes search and retrieves the search result given the query batch (from, size)
  private final Function<QueryPagination, SearchResult> searcher;
  // Function that generates the cache key given the query batch (from, size)
  private final Function<QueryPagination, K> cacheKeyGenerator;
  @Nullable
  private final SearchFlags searchFlags;
  private final boolean enableCache;

  @Value
  public static class QueryPagination {
    int from;
    int size;
  }

  /**
   * Get search results corresponding to the input "from" and "size"
   * It goes through batches, starting from the beginning, until we get enough results to return
   * This let's us have batches that return a variable number of results (we have no idea which batch the "from" "size" page corresponds to)
   */
  public SearchResult getSearchResults(int from, int size) {
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "getSearchResults").time()) {
      int resultsSoFar = 0;
      int batchId = 0;
      boolean foundStart = false;
      List<SearchEntity> resultEntities = new ArrayList<>();
      SearchResult batchedResult;
      // Use do-while to make sure we run at least one batch to fetch metadata
      do {
        batchedResult = getBatch(batchId);
        int currentBatchSize = batchedResult.getEntities().size();
        // If the number of results in this batch is 0, no need to continue
        if (currentBatchSize == 0) {
          break;
        }
        if (resultsSoFar + currentBatchSize > from) {
          int startInBatch = foundStart ? 0 : from - resultsSoFar;
          int endInBatch = Math.min(currentBatchSize, startInBatch + size - resultEntities.size());
          resultEntities.addAll(batchedResult.getEntities().subList(startInBatch, endInBatch));
          foundStart = true;
        }
        // If current batch is smaller than the requested batch size, the next batch will return empty.
        if (currentBatchSize < batchSize) {
          break;
        }
        resultsSoFar += currentBatchSize;
        batchId++;
      } while (resultsSoFar < from + size);
      return new SearchResult().setEntities(new SearchEntityArray(resultEntities))
          .setMetadata(batchedResult.getMetadata())
          .setFrom(from)
          .setPageSize(size)
          .setNumEntities(batchedResult.getNumEntities());
    }
  }

  private QueryPagination getBatchQuerySize(int batchId) {
    return new QueryPagination(batchId * batchSize, batchSize);
  }

  private SearchResult getBatch(int batchId) {
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "getBatch").time()) {
      QueryPagination batch = getBatchQuerySize(batchId);
      SearchResult result;
      if (enableCache()) {
        Timer.Context cacheAccess = MetricUtils.timer(this.getClass(), "getBatch_cache_access").time();
        K cacheKey = cacheKeyGenerator.apply(batch);
        result = cache.get(cacheKey, SearchResult.class);
        cacheAccess.stop();
        if (result == null) {
          Timer.Context cacheMiss = MetricUtils.timer(this.getClass(), "getBatch_cache_miss").time();
          result = searcher.apply(batch);
          cache.put(cacheKey, result);
          cacheMiss.stop();
          MetricUtils.counter(this.getClass(), "getBatch_cache_miss_count").inc();
        }
      } else {
        result = searcher.apply(batch);
      }
      return result;
    }
  }

  private boolean enableCache() {
    return enableCache && (searchFlags == null || !searchFlags.isSkipCache());
  }
}
