package com.linkedin.metadata.search.client;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.cache.CacheableSearcher;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.opentelemetry.api.trace.Span;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CachingEntitySearchServiceTest {

  @Mock private CacheManager cacheManager;

  @Mock private EntitySearchService entitySearchService;

  private OperationContext opContext;

  @Mock private SearchContext searchContext;

  @Mock private Cache searchCache;

  @Mock private Cache autoCompleteCache;

  @Mock private Cache browseCache;

  @Mock private Cache scrollCache;

  @Mock private MetricUtils metricUtils;

  @Mock private Span span;

  private CachingEntitySearchService cachingService;

  private static final int BATCH_SIZE = 100;
  private static final boolean ENABLE_CACHE = true;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    opContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () -> SystemTelemetryContext.TEST.toBuilder().metricUtils(metricUtils).build())
            .buildSystemContext();

    // Setup cache manager
    when(cacheManager.getCache(CachingEntitySearchService.ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME))
        .thenReturn(searchCache);
    when(cacheManager.getCache(
            CachingEntitySearchService.ENTITY_SEARCH_SERVICE_AUTOCOMPLETE_CACHE_NAME))
        .thenReturn(autoCompleteCache);
    when(cacheManager.getCache(CachingEntitySearchService.ENTITY_SEARCH_SERVICE_BROWSE_CACHE_NAME))
        .thenReturn(browseCache);
    when(cacheManager.getCache(CachingEntitySearchService.ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME))
        .thenReturn(scrollCache);

    // Setup static mocks
    try (MockedStatic<Span> spanMock = mockStatic(Span.class)) {
      spanMock.when(Span::current).thenReturn(span);
    }

    when(entitySearchService.getSearchServiceConfig()).thenReturn(TEST_SEARCH_SERVICE_CONFIG);

    cachingService =
        new CachingEntitySearchService(cacheManager, entitySearchService, BATCH_SIZE, ENABLE_CACHE);
  }

  @Test
  public void testSearchWithCacheHit() {
    // Arrange
    List<String> entityNames = Arrays.asList("dataset", "chart");
    String query = "test query";
    Filter filter = new Filter();
    List<SortCriterion> sortCriteria = Collections.emptyList();
    int from = 0;
    Integer size = 10;
    List<String> facets = Arrays.asList("platform", "origin");

    when(searchCache.get(any(), eq(String.class)))
        .thenReturn("{\"entities\":[],\"metadata\":{},\"numEntities\":0}");

    // Act
    SearchResult result =
        cachingService.search(
            opContext, entityNames, query, filter, sortCriteria, from, size, facets);

    // Assert
    assertNotNull(result);
    verify(entitySearchService, never())
        .search(any(), any(), any(), any(), any(), anyInt(), any(), any());
  }

  @Test
  public void testSearchWithCacheMiss() {
    // Arrange
    List<String> entityNames = Arrays.asList("dataset");
    String query = "test query";
    Filter filter = null;
    List<SortCriterion> sortCriteria = Collections.emptyList();
    int from = 0;
    Integer size = 10;
    List<String> facets = Collections.emptyList();

    SearchResult expectedResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setPageSize(size)
            .setMetadata(new SearchResultMetadata())
            .setFrom(0);
    when(searchCache.get(any(), eq(String.class))).thenReturn(null);
    when(entitySearchService.search(any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(expectedResult);

    // Act
    SearchResult result =
        cachingService.search(
            opContext, entityNames, query, filter, sortCriteria, from, size, facets);

    // Assert
    assertNotNull(result);
    assertEquals(result, expectedResult);
    verify(entitySearchService)
        .search(
            eq(opContext),
            eq(entityNames),
            eq(query),
            eq(filter),
            eq(sortCriteria),
            eq(from),
            eq(100),
            eq(facets));
    verify(searchCache).put(any(), anyString());
    verify(metricUtils)
        .increment(eq(CacheableSearcher.class), eq("getBatch_cache_miss_count"), eq(1d));
  }

  @Test
  public void testSearchWithCacheDisabled() {
    // Arrange
    CachingEntitySearchService serviceWithCacheDisabled =
        new CachingEntitySearchService(cacheManager, entitySearchService, BATCH_SIZE, false);

    List<String> entityNames = Arrays.asList("dataset");
    String query = "test";
    SearchResult expectedResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setMetadata(new SearchResultMetadata())
            .setNumEntities(0)
            .setPageSize(10)
            .setFrom(0);

    when(entitySearchService.search(any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(expectedResult);

    // Act
    SearchResult result =
        serviceWithCacheDisabled.search(
            opContext, entityNames, query, null, null, 0, 10, Collections.emptyList());

    // Assert
    assertEquals(result, expectedResult);
    verify(searchCache, never()).get(any(), (Class<String>) any());
    verify(searchCache, never()).put(any(), any());
  }

  @Test
  public void testAutoCompleteWithCacheHit() {
    // Arrange
    String entityName = "dataset";
    String input = "test";
    String field = "name";
    Filter filter = new Filter();
    Integer limit = 10;

    AutoCompleteResult cachedResult = new AutoCompleteResult();
    when(autoCompleteCache.get(any(), eq(String.class))).thenReturn("{\"query\":\"test\"}");

    // Act
    AutoCompleteResult result =
        cachingService.autoComplete(opContext, entityName, input, field, filter, limit);

    // Assert
    assertNotNull(result);
    verify(entitySearchService, never()).autoComplete(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testAutoCompleteWithCacheMiss() {
    // Arrange
    String entityName = "dataset";
    String input = "test";
    String field = null;
    Filter filter = null;
    Integer limit = 5;

    AutoCompleteResult expectedResult = new AutoCompleteResult();
    when(autoCompleteCache.get(any(), eq(String.class))).thenReturn(null);
    when(entitySearchService.autoComplete(any(), any(), any(), any(), any(), any()))
        .thenReturn(expectedResult);

    // Act
    AutoCompleteResult result =
        cachingService.autoComplete(opContext, entityName, input, field, filter, limit);

    // Assert
    assertNotNull(result);
    assertEquals(result, expectedResult);
    verify(entitySearchService)
        .autoComplete(eq(opContext), eq(entityName), eq(input), eq(field), eq(filter), eq(limit));
    verify(autoCompleteCache).put(any(), anyString());
    verify(metricUtils)
        .increment(
            eq(CachingEntitySearchService.class), eq("autocomplete_cache_miss_count"), eq(1d));
  }

  @Test
  public void testBrowseWithCacheHit() {
    // Arrange
    String entityName = "dataset";
    String path = "/prod/data";
    Filter filter = new Filter();
    int from = 0;
    Integer size = 20;

    BrowseResult cachedResult = new BrowseResult();
    when(browseCache.get(any(), eq(String.class))).thenReturn("{\"entities\":[]}");

    // Act
    BrowseResult result = cachingService.browse(opContext, entityName, path, filter, from, size);

    // Assert
    assertNotNull(result);
    verify(entitySearchService, never()).browse(any(), any(), any(), any(), anyInt(), any());
  }

  @Test
  public void testBrowseWithCacheMiss() {
    // Arrange
    String entityName = "dataset";
    String path = "/prod";
    Filter filter = null;
    int from = 10;
    Integer size = 50;

    BrowseResult expectedResult = new BrowseResult();
    when(browseCache.get(any(), eq(String.class))).thenReturn(null);
    when(entitySearchService.browse(any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(expectedResult);

    // Act
    BrowseResult result = cachingService.browse(opContext, entityName, path, filter, from, size);

    // Assert
    assertNotNull(result);
    assertEquals(result, expectedResult);
    verify(entitySearchService)
        .browse(eq(opContext), eq(entityName), eq(path), eq(filter), eq(from), eq(size));
    verify(browseCache).put(any(), anyString());
    verify(metricUtils)
        .increment(eq(CachingEntitySearchService.class), eq("browse_cache_miss_count"), eq(1d));
  }

  @Test
  public void testScrollWithCacheHit() {
    // Arrange
    List<String> entities = Arrays.asList("dataset", "dataflow");
    String query = "scroll test";
    Filter filter = new Filter();
    List<SortCriterion> sortCriteria = Collections.emptyList();
    String scrollId = "scroll123";
    String keepAlive = "1m";
    Integer size = 100;
    List<String> facets = Arrays.asList("platform");

    ScrollResult cachedResult = new ScrollResult();
    when(scrollCache.get(any(), eq(String.class))).thenReturn("{\"entities\":[]}");

    // Act
    ScrollResult result =
        cachingService.scroll(
            opContext, entities, query, filter, sortCriteria, scrollId, keepAlive, size, facets);

    // Assert
    assertNotNull(result);
    verify(entitySearchService, never())
        .fullTextScroll(any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(entitySearchService, never())
        .structuredScroll(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testScrollWithCacheMissFullText() {
    // Arrange
    List<String> entities = Arrays.asList("dataset");
    String query = "test";
    Filter filter = null;
    List<SortCriterion> sortCriteria = null;
    String scrollId = null;
    String keepAlive = "5m";
    Integer size = 50;
    List<String> facets = Collections.emptyList();

    ScrollResult expectedResult =
        new ScrollResult().setEntities(new SearchEntityArray()).setNumEntities(0).setPageSize(size);
    when(scrollCache.get(any(), eq(String.class))).thenReturn(null);
    when(entitySearchService.fullTextScroll(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(expectedResult);

    // Act
    ScrollResult result =
        cachingService.scroll(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            entities,
            query,
            filter,
            sortCriteria,
            scrollId,
            keepAlive,
            size,
            facets);

    // Assert
    assertNotNull(result);
    assertEquals(result, expectedResult);
    verify(entitySearchService)
        .fullTextScroll(
            any(OperationContext.class),
            eq(entities),
            eq(query),
            eq(filter),
            eq(sortCriteria),
            eq(scrollId),
            eq(keepAlive),
            eq(size),
            eq(facets));
    verify(entitySearchService, never())
        .structuredScroll(any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(scrollCache).put(any(), anyString());
    verify(metricUtils)
        .increment(eq(CachingEntitySearchService.class), eq("scroll_cache_miss_count"), eq(1d));
  }

  @Test
  public void testScrollWithCacheMissStructured() {
    // Arrange
    List<String> entities = Arrays.asList("dataset");
    String query = "test";
    SearchFlags searchFlags = new SearchFlags();
    searchFlags.setFulltext(false);
    when(searchContext.getSearchFlags()).thenReturn(searchFlags);

    ScrollResult expectedResult = new ScrollResult();
    when(scrollCache.get(any(), eq(String.class))).thenReturn(null);
    when(entitySearchService.structuredScroll(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(expectedResult);

    // Act
    ScrollResult result =
        cachingService.scroll(
            opContext, entities, query, null, null, null, "1m", 10, Collections.emptyList());

    // Assert
    assertNotNull(result);
    assertEquals(result, expectedResult);
    verify(entitySearchService)
        .structuredScroll(any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(entitySearchService, never())
        .fullTextScroll(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testCacheSkipWithSearchFlags() {
    // Arrange
    String entityName = "dataset";
    String input = "test";
    AutoCompleteResult expectedResult = new AutoCompleteResult();

    when(entitySearchService.autoComplete(any(), any(), any(), any(), any(), any()))
        .thenReturn(expectedResult);

    // Act
    AutoCompleteResult result =
        cachingService.autoComplete(
            opContext.withSearchFlags(flags -> flags.setSkipCache(true)),
            entityName,
            input,
            null,
            null,
            10);

    // Assert
    assertEquals(result, expectedResult);
    verify(autoCompleteCache, never()).get(any(), (Class<String>) any());
    verify(autoCompleteCache, never()).put(any(), any());
    verify(entitySearchService).autoComplete(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testNullSearchFlags() {
    // Arrange
    when(searchContext.getSearchFlags()).thenReturn(null);

    String entityName = "dataset";
    String path = "/test";
    BrowseResult expectedResult = new BrowseResult();

    when(browseCache.get(any(), eq(String.class))).thenReturn(null);
    when(entitySearchService.browse(any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(expectedResult);

    // Act
    BrowseResult result = cachingService.browse(opContext, entityName, path, null, 0, 10);

    // Assert
    assertNotNull(result);
    verify(browseCache).get(any(), eq(String.class));
    verify(browseCache).put(any(), anyString());
  }

  @Test
  public void testCacheKeyGeneration() {
    // Test that different parameters generate different cache keys
    String query1 = "test1";
    String query2 = "test2";

    when(searchCache.get(any(), eq(String.class))).thenReturn(null);
    SearchResult result =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setMetadata(new SearchResultMetadata())
            .setNumEntities(0)
            .setPageSize(10);
    when(entitySearchService.search(any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(result);

    // First search
    cachingService.search(
        opContext, Arrays.asList("dataset"), query1, null, null, 0, 10, Collections.emptyList());

    // Second search with different query
    cachingService.search(
        opContext, Arrays.asList("dataset"), query2, null, null, 0, 10, Collections.emptyList());

    // Verify two different cache keys were used
    verify(searchCache, times(2)).get(any(), eq(String.class));
    verify(searchCache, times(2)).put(any(), anyString());
    verify(entitySearchService, times(2))
        .search(any(), any(), any(), any(), any(), anyInt(), any(), any());
  }
}
