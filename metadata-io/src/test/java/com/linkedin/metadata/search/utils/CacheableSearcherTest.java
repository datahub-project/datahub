package com.linkedin.metadata.search.utils;

import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


public class CacheableSearcherTest {
  private CacheManager cacheManager = new ConcurrentMapCacheManager();

  @Test
  public void testBatchSearcherWhenEmpty() {
    CacheableSearcher<Integer> emptySearcher =
        new CacheableSearcher<>(cacheManager.getCache("emptySearcher"), 10, this::getEmptySearchResult,
            CacheableSearcher.QuerySize::getFrom);
    assertTrue(emptySearcher.getSearchResults(0, 0).getEntities().isEmpty());
    assertTrue(emptySearcher.getSearchResults(0, 10).getEntities().isEmpty());
    assertTrue(emptySearcher.getSearchResults(5, 10).getEntities().isEmpty());
  }

  private SearchResult getEmptySearchResult(CacheableSearcher.QuerySize querySize) {
    return new SearchResult().setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(querySize.getFrom())
        .setPageSize(querySize.getSize())
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  private SearchResult getSearchResult(CacheableSearcher.QuerySize querySize, int batchSize) {
    assert (batchSize <= querySize.getSize());
    List<SearchEntity> entities = IntStream.range(0, batchSize)
        .mapToObj(i -> new SearchEntity().setEntity(new TestEntityUrn(i + "", "", "")))
        .collect(Collectors.toList());
    return new SearchResult().setEntities(new SearchEntityArray(entities))
        .setNumEntities(10000)
        .setFrom(querySize.getFrom())
        .setPageSize(querySize.getSize())
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }
}
