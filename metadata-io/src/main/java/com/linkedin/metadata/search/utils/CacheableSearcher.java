package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.springframework.cache.Cache;


/**
 * Utility for searching in batches and caching the results.
 */
@RequiredArgsConstructor
public class CacheableSearcher<K> {
  @Nonnull
  private final Cache cache;
  private final int batchSize;
  private final Function<QuerySize, SearchResult> searcher;
  private final Function<QuerySize, K> cacheKeyGenerator;

  @Value
  public static class QuerySize {
    int from;
    int size;
  }

  public SearchResult getSearchResults(int from, int size) {
    int resultsSoFar = 0;
    int batchId = 0;
    boolean foundStart = false;
    List<SearchEntity> resultEntities = new ArrayList<>();
    SearchResult batchedResult = null;
    while (resultsSoFar < from + size) {
      batchedResult = getBatch(batchId);
      int currentBatchSize = batchedResult.getEntities().size();
      // If the number of results is less than "from", should return empty
      // If the number of results in this batch is 0, no need to continue
      if (batchedResult.getNumEntities() < from || currentBatchSize == 0) {
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
    }
    return new SearchResult().setEntities(new SearchEntityArray(resultEntities))
        .setMetadata(batchedResult == null ? new SearchResultMetadata() : batchedResult.getMetadata())
        .setFrom(from)
        .setPageSize(size)
        .setNumEntities(batchedResult == null ? 0 : batchedResult.getNumEntities());
  }

  private QuerySize getBatchQuerySize(int batchId) {
    return new QuerySize(batchId * batchSize, batchSize);
  }

  private SearchResult getBatch(int batchId) {
    QuerySize batch = getBatchQuerySize(batchId);
    K cacheKey = cacheKeyGenerator.apply(batch);
    SearchResult result = cache.get(cacheKey, SearchResult.class);
    if (result == null) {
      result = searcher.apply(batch);
      cache.put(cacheKey, result);
    }
    return result;
  }
}
