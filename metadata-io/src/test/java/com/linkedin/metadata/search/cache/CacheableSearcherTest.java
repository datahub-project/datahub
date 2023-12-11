package com.linkedin.metadata.search.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Streams;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.Mockito;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.testng.annotations.Test;

public class CacheableSearcherTest {
  private CacheManager cacheManager = new ConcurrentMapCacheManager();

  @Test
  public void testCacheableSearcherWhenEmpty() {
    CacheableSearcher<Integer> emptySearcher =
        new CacheableSearcher<>(
            cacheManager.getCache("emptySearcher"),
            10,
            this::getEmptySearchResult,
            CacheableSearcher.QueryPagination::getFrom,
            null,
            true);
    assertTrue(emptySearcher.getSearchResults(0, 0).getEntities().isEmpty());
    assertTrue(emptySearcher.getSearchResults(0, 10).getEntities().isEmpty());
    assertTrue(emptySearcher.getSearchResults(5, 10).getEntities().isEmpty());
  }

  @Test
  public void testCacheableSearcherWithFixedNumResults() {
    CacheableSearcher<Integer> fixedBatchSearcher =
        new CacheableSearcher<>(
            cacheManager.getCache("fixedBatchSearcher"),
            10,
            qs -> getSearchResult(qs, 10),
            CacheableSearcher.QueryPagination::getFrom,
            null,
            true);

    SearchResult result = fixedBatchSearcher.getSearchResults(0, 0);
    assertTrue(result.getEntities().isEmpty());
    assertEquals(result.getNumEntities().intValue(), 1000);

    result = fixedBatchSearcher.getSearchResults(0, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        getUrns(0, 10));

    result = fixedBatchSearcher.getSearchResults(5, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        Streams.concat(getUrns(5, 10).stream(), getUrns(0, 5).stream())
            .collect(Collectors.toList()));
  }

  @Test
  public void testCacheableSearcherWithVariableNumResults() {
    CacheableSearcher<Integer> variableBatchSearcher =
        new CacheableSearcher<>(
            cacheManager.getCache("variableBatchSearcher"),
            10,
            qs -> getSearchResult(qs, qs.getFrom() + qs.getSize()),
            CacheableSearcher.QueryPagination::getFrom,
            null,
            true);

    SearchResult result = variableBatchSearcher.getSearchResults(0, 0);
    assertTrue(result.getEntities().isEmpty());
    assertEquals(result.getNumEntities().intValue(), 1000);

    result = variableBatchSearcher.getSearchResults(0, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        getUrns(0, 10));

    result = variableBatchSearcher.getSearchResults(5, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        Streams.concat(getUrns(5, 10).stream(), getUrns(0, 5).stream())
            .collect(Collectors.toList()));

    result = variableBatchSearcher.getSearchResults(5, 100);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 100);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        Streams.concat(
                getUrns(5, 10).stream(),
                getUrns(0, 20).stream(),
                getUrns(0, 30).stream(),
                getUrns(0, 40).stream(),
                getUrns(0, 5).stream())
            .collect(Collectors.toList()));
  }

  @Test
  public void testCacheableSearcherEnabled() {
    // Verify cache is not interacted with when cache disabled
    Cache mockCache = Mockito.mock(Cache.class);
    CacheableSearcher<Integer> cacheDisabled =
        new CacheableSearcher<>(
            mockCache,
            10,
            qs -> getSearchResult(qs, qs.getFrom() + qs.getSize()),
            CacheableSearcher.QueryPagination::getFrom,
            null,
            false);
    SearchResult result = cacheDisabled.getSearchResults(0, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        getUrns(0, 10));
    Mockito.verifyNoInteractions(mockCache);
    Mockito.reset(mockCache);

    // Verify cache is updated when cache enabled, but skip cache passed through
    CacheableSearcher<Integer> skipCache =
        new CacheableSearcher<>(
            mockCache,
            10,
            qs -> getSearchResult(qs, qs.getFrom() + qs.getSize()),
            CacheableSearcher.QueryPagination::getFrom,
            new SearchFlags().setSkipCache(true),
            true);
    result = skipCache.getSearchResults(0, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        getUrns(0, 10));
    Mockito.verify(mockCache, Mockito.times(1)).put(Mockito.any(), Mockito.any());
    Mockito.verify(mockCache, Mockito.times(0)).get(Mockito.any(), Mockito.any(Class.class));
    Mockito.reset(mockCache);

    // Test cache hit when searchFlags is null
    CacheableSearcher<Integer> nullFlags =
        new CacheableSearcher<>(
            mockCache,
            10,
            qs -> getSearchResult(qs, qs.getFrom() + qs.getSize()),
            CacheableSearcher.QueryPagination::getFrom,
            null,
            true);
    result = nullFlags.getSearchResults(0, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        getUrns(0, 10));
    Mockito.verify(mockCache, Mockito.times(1)).put(Mockito.any(), Mockito.any());
    Mockito.verify(mockCache, Mockito.times(1)).get(Mockito.any(), Mockito.any(Class.class));
    Mockito.reset(mockCache);

    // Test cache hit when skipCache is false
    CacheableSearcher<Integer> useCache =
        new CacheableSearcher<>(
            mockCache,
            10,
            qs -> getSearchResult(qs, qs.getFrom() + qs.getSize()),
            CacheableSearcher.QueryPagination::getFrom,
            new SearchFlags().setSkipCache(false),
            true);
    result = useCache.getSearchResults(0, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        getUrns(0, 10));
    Mockito.verify(mockCache, Mockito.times(1)).put(Mockito.any(), Mockito.any());
    Mockito.verify(mockCache, Mockito.times(1)).get(Mockito.any(), Mockito.any(Class.class));
  }

  private SearchResult getEmptySearchResult(CacheableSearcher.QueryPagination queryPagination) {
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(queryPagination.getFrom())
        .setPageSize(queryPagination.getSize())
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  private List<Urn> getUrns(int start, int end) {
    return IntStream.range(start, end)
        .mapToObj(i -> new TestEntityUrn(i + "", "test", "test"))
        .collect(Collectors.toList());
  }

  private SearchResult getSearchResult(
      CacheableSearcher.QueryPagination queryPagination, int batchSize) {
    assert (batchSize <= queryPagination.getSize());
    List<SearchEntity> entities =
        getUrns(0, batchSize).stream()
            .map(urn -> new SearchEntity().setEntity(urn))
            .collect(Collectors.toList());
    return new SearchResult()
        .setEntities(new SearchEntityArray(entities))
        .setNumEntities(1000)
        .setFrom(queryPagination.getFrom())
        .setPageSize(queryPagination.getSize())
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }
}
