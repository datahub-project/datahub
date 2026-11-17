package com.linkedin.metadata.search.cache;

import static com.datahub.util.RecordUtils.*;
import static com.linkedin.metadata.utils.metrics.MetricUtils.CACHE_HIT_ATTR;

import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.springframework.cache.Cache;

/** Wrapper class to allow searching in batches and caching the results. */
@RequiredArgsConstructor
public class CacheableSearcher<K> {
  @Nonnull private final Cache cache;
  private final int batchSize;
  // Function that executes search and retrieves the search result given the query batch (from,
  // size)
  private final Function<QueryPagination, SearchResult> searcher;
  // Function that generates the cache key given the query batch (from, size)
  private final Function<QueryPagination, K> cacheKeyGenerator;
  private final boolean enableCache;

  @Value
  public static class QueryPagination implements Serializable {
    int from;
    int size;
  }

  /**
   * Get search results corresponding to the input "from" and "size" It goes through batches,
   * starting from the beginning, until we get enough results to return This let's us have batches
   * that return a variable number of results (we have no idea which batch the "from" "size" page
   * corresponds to)
   */
  public SearchResult getSearchResults(@Nonnull OperationContext opContext, int from, int size) {
    return opContext.withSpan(
        "getSearchResults",
        () -> {
          int resultsSoFar = 0;
          int batchId = 0;
          boolean foundStart = false;
          List<SearchEntity> resultEntities = new ArrayList<>();
          SearchResultMetadata startMetadata = null;
          SearchResult batchedResult;
          // Use do-while to make sure we run at least one batch to fetch metadata
          do {
            batchedResult = getBatch(opContext, batchId);
            int currentBatchSize = batchedResult.getEntities().size();
            // If the number of results in this batch is 0, no need to continue
            if (currentBatchSize == 0) {
              break;
            }
            if (resultsSoFar + currentBatchSize > from) {
              if (!foundStart) {
                startMetadata = batchedResult.getMetadata();
              }
              int startInBatch = foundStart ? 0 : from - resultsSoFar;
              int endInBatch =
                  Math.min(currentBatchSize, startInBatch + size - resultEntities.size());
              resultEntities.addAll(batchedResult.getEntities().subList(startInBatch, endInBatch));
              foundStart = true;
            }
            // Stop once the search engine has no more matching documents beyond this batch.
            //
            // We compare the raw hit positions scanned so far ((batchId + 1) * batchSize) against
            // the total hit count, rather than checking (currentBatchSize < batchSize). Hits with
            // invalid/missing URNs are dropped before reaching here (see
            // SearchRequestHandler#getResultSafely), so a full page of batchSize raw hits can yield
            // fewer than batchSize entities. Using the entity count to detect the last page would
            // either silently truncate results when a hit is skipped, or force an unnecessary extra
            // batch fetch. The total hit count is exact within the from/size pagination window
            // (which the search engine caps at index.max_result_window).
            if ((long) (batchId + 1) * batchSize >= batchedResult.getNumEntities()) {
              break;
            }
            resultsSoFar += currentBatchSize;
            batchId++;
          } while (resultsSoFar < from + size);
          SearchResultMetadata metadata = batchedResult.getMetadata();
          // A rerank explains the batch it reordered, so a page that spans batches keeps the
          // explanation of the batch that holds its first row
          if (metadata != null
              && startMetadata != null
              && startMetadata != metadata
              && startMetadata.hasKeywordRerank()) {
            metadata.setKeywordRerank(startMetadata.getKeywordRerank());
          }
          return new SearchResult()
              .setEntities(new SearchEntityArray(resultEntities))
              .setMetadata(metadata)
              .setFrom(from)
              .setPageSize(size)
              .setNumEntities(batchedResult.getNumEntities());
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getSearchResults"));
  }

  private QueryPagination getBatchQuerySize(int batchId) {
    return new QueryPagination(batchId * batchSize, batchSize);
  }

  private SearchResult getBatch(@Nonnull OperationContext opContext, int batchId) {

    return opContext.withSpan(
        "getBatch",
        () -> {
          QueryPagination batch = getBatchQuerySize(batchId);
          SearchResult result;
          if (enableCache) {
            K cacheKey = cacheKeyGenerator.apply(batch);
            if ((opContext.getSearchContext().getSearchFlags().isSkipCache() == null
                || !opContext.getSearchContext().getSearchFlags().isSkipCache())) {

              String json = cache.get(cacheKey, String.class);
              result = json != null ? toRecordTemplate(SearchResult.class, json) : null;

              if (result == null) {
                Span.current().setAttribute(CACHE_HIT_ATTR, false);
                result = searcher.apply(batch);
                cache.put(cacheKey, toJsonString(result));
                opContext
                    .getMetricUtils()
                    .ifPresent(
                        metricUtils ->
                            metricUtils.increment(this.getClass(), "getBatch_cache_miss_count", 1));
              } else {
                Span.current().setAttribute(CACHE_HIT_ATTR, true);
              }

            } else {
              Span.current().setAttribute(CACHE_HIT_ATTR, false);
              result = searcher.apply(batch);
              cache.put(cacheKey, toJsonString(result));
            }
          } else {
            Span.current().setAttribute(CACHE_HIT_ATTR, false);
            result = searcher.apply(batch);
          }
          return result;
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getBatch"));
  }
}
