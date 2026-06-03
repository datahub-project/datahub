package com.linkedin.metadata.search.cache;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Streams;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));
    CacheableSearcher<Integer> emptySearcher =
        new CacheableSearcher<>(
            cacheManager.getCache("emptySearcher"),
            10,
            this::getEmptySearchResult,
            CacheableSearcher.QueryPagination::getFrom,
            true);
    assertTrue(emptySearcher.getSearchResults(opContext, 0, 0).getEntities().isEmpty());
    assertTrue(emptySearcher.getSearchResults(opContext, 0, 10).getEntities().isEmpty());
    assertTrue(emptySearcher.getSearchResults(opContext, 5, 10).getEntities().isEmpty());
  }

  @Test
  public void testCacheableSearcherWithFixedNumResults() {
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));
    CacheableSearcher<Integer> fixedBatchSearcher =
        new CacheableSearcher<>(
            cacheManager.getCache("fixedBatchSearcher"),
            10,
            qs -> getSearchResult(qs, 10),
            CacheableSearcher.QueryPagination::getFrom,
            true);

    SearchResult result = fixedBatchSearcher.getSearchResults(opContext, 0, 0);
    assertTrue(result.getEntities().isEmpty());
    assertEquals(result.getNumEntities().intValue(), 1000);

    result = fixedBatchSearcher.getSearchResults(opContext, 0, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        getUrns(0, 10));

    result = fixedBatchSearcher.getSearchResults(opContext, 5, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        Streams.concat(getUrns(5, 10).stream(), getUrns(0, 5).stream())
            .collect(Collectors.toList()));
  }

  @Test
  public void testCacheableSearcherWithVariableNumResults() {
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));
    CacheableSearcher<Integer> variableBatchSearcher =
        new CacheableSearcher<>(
            cacheManager.getCache("variableBatchSearcher"),
            10,
            qs -> getSearchResult(qs, qs.getFrom() + qs.getSize()),
            CacheableSearcher.QueryPagination::getFrom,
            true);

    SearchResult result = variableBatchSearcher.getSearchResults(opContext, 0, 0);
    assertTrue(result.getEntities().isEmpty());
    assertEquals(result.getNumEntities().intValue(), 1000);

    result = variableBatchSearcher.getSearchResults(opContext, 0, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        getUrns(0, 10));

    result = variableBatchSearcher.getSearchResults(opContext, 5, 10);
    assertEquals(result.getNumEntities().intValue(), 1000);
    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        Streams.concat(getUrns(5, 10).stream(), getUrns(0, 5).stream())
            .collect(Collectors.toList()));

    result = variableBatchSearcher.getSearchResults(opContext, 5, 100);
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
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));
    // Verify cache is not interacted with when cache disabled
    Cache mockCache = mock(Cache.class);
    CacheableSearcher<Integer> cacheDisabled =
        new CacheableSearcher<>(
            mockCache,
            10,
            qs -> getSearchResult(qs, qs.getFrom() + qs.getSize()),
            CacheableSearcher.QueryPagination::getFrom,
            false);
    SearchResult result = cacheDisabled.getSearchResults(opContext, 0, 10);
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
            true);
    result =
        skipCache.getSearchResults(
            opContext.withSearchFlags(flags -> flags.setSkipCache(true)), 0, 10);
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
            true);
    result =
        nullFlags.getSearchResults(opContext.withSearchFlags(flags -> new SearchFlags()), 0, 10);
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
            true);
    result =
        useCache.getSearchResults(
            opContext.withSearchFlags(flags -> flags.setSkipCache(false)), 0, 10);
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

  @Test
  public void testCacheableSearcherDoesNotTruncateWhenHitsAreSkipped() {
    // Regression test (follow-up to #13181): SearchRequestHandler drops hits with invalid/missing
    // URNs, so a batch can return fewer entities than the batch size while more results remain.
    // The searcher must keep paging in that case rather than treating the short batch as the end.
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));
    int batchSize = 10;
    // 15 total hits across two batches; one hit in batch 0 is dropped as invalid -> 14 entities.
    int totalHits = 15;
    CacheableSearcher<Integer> skippedHitSearcher =
        new CacheableSearcher<>(
            cacheManager.getCache("skippedHitSearcher"),
            batchSize,
            qs -> {
              // Batch 0 [0,10): a full page of 10 raw hits, but one was dropped as invalid -> 9.
              // Batch 1 [10,20): 5 more valid entities.
              if (qs.getFrom() == 0) {
                return searchResultWithUrns(qs, getUrns(0, 9), totalHits);
              } else if (qs.getFrom() == 10) {
                return searchResultWithUrns(qs, getUrns(10, 15), totalHits);
              }
              return searchResultWithUrns(qs, List.of(), totalHits);
            },
            CacheableSearcher.QueryPagination::getFrom,
            true);

    SearchResult result = skippedHitSearcher.getSearchResults(opContext, 0, 100);

    // Before the fix the loop stopped after batch 0 (9 < batchSize) and returned only 9 entities,
    // silently dropping the 5 results in batch 1.
    assertEquals(result.getEntities().size(), 14);
    assertEquals(
        result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        Streams.concat(getUrns(0, 9).stream(), getUrns(10, 15).stream())
            .collect(Collectors.toList()));
  }

  @Test
  public void testCacheableSearcherDoesNotOverFetchWhenResultsSmallerThanWindow() {
    // Regression guard: a result set smaller than the requested window must be served from a
    // single batch fetch. Detecting the last page via the entity count would force an extra
    // (empty) search call here, which broke the lineage single-call optimization.
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mock(EntityRegistry.class));
    AtomicInteger fetches = new AtomicInteger();
    CacheableSearcher<Integer> searcher =
        new CacheableSearcher<>(
            mock(Cache.class),
            10,
            qs -> {
              fetches.incrementAndGet();
              return searchResultWithUrns(qs, getUrns(0, 3), 3);
            },
            CacheableSearcher.QueryPagination::getFrom,
            false);

    SearchResult result = searcher.getSearchResults(opContext, 0, 100);

    assertEquals(result.getEntities().size(), 3);
    assertEquals(
        fetches.get(), 1, "result set smaller than the window must require only one batch fetch");
  }

  private SearchResult searchResultWithUrns(
      CacheableSearcher.QueryPagination queryPagination, List<Urn> urns, int numEntities) {
    List<SearchEntity> entities =
        urns.stream().map(urn -> new SearchEntity().setEntity(urn)).collect(Collectors.toList());
    return new SearchResult()
        .setEntities(new SearchEntityArray(entities))
        .setNumEntities(numEntities)
        .setFrom(queryPagination.getFrom())
        .setPageSize(queryPagination.getSize())
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }
}
