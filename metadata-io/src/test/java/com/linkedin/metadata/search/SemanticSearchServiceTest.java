package com.linkedin.metadata.search;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for SemanticSearchService */
public class SemanticSearchServiceTest {

  private SemanticSearchService semanticSearchService;
  private EntityDocCountCache mockEntityDocCountCache;
  private CachingEntitySearchService mockCachingEntitySearchService;
  private SemanticEntitySearch mockSemanticEntitySearchService;
  private SearchServiceConfiguration mockSearchServiceConfig;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockEntityDocCountCache = mock(EntityDocCountCache.class);
    mockCachingEntitySearchService = mock(CachingEntitySearchService.class);
    mockSemanticEntitySearchService = mock(SemanticEntitySearch.class);
    mockSearchServiceConfig =
        SearchServiceConfiguration.builder()
            .limit(
                LimitConfig.builder()
                    .results(ResultsLimitConfig.builder().apiDefault(100).max(1000).build())
                    .build())
            .semanticSearchEnabled(true)
            .build();

    semanticSearchService =
        new SemanticSearchService(
            mockEntityDocCountCache,
            mockCachingEntitySearchService,
            mockSemanticEntitySearchService,
            mockSearchServiceConfig);

    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testSemanticSearch() {
    // Given
    List<String> entityNames = Collections.singletonList("dataset");
    String query = "machine learning models";
    int from = 0;
    Integer size = 10;

    SearchResult mockResult = createMockSearchResult(from, size, 5);
    when(mockSemanticEntitySearchService.search(
            any(), anyList(), anyString(), any(), any(), anyInt(), any()))
        .thenReturn(mockResult);

    when(mockEntityDocCountCache.getNonEmptyEntities(any()))
        .thenReturn(Collections.singletonList("dataset"));

    // When
    SearchResult result =
        semanticSearchService.semanticSearch(opContext, entityNames, query, null, null, from, size);

    // Then
    assertNotNull(result);
    assertEquals(result.getFrom().intValue(), from);
    assertEquals(result.getPageSize().intValue(), size);
    assertEquals(result.getNumEntities().intValue(), 5);

    // Verify semantic service was called
    verify(mockSemanticEntitySearchService, times(1))
        .search(eq(opContext), eq(entityNames), eq(query), eq(null), eq(null), eq(from), eq(size));
  }

  @Test
  public void testSemanticSearchAcrossEntities() {
    // Given
    List<String> entityNames = Arrays.asList("dataset", "chart");
    String query = "customer analytics";
    int from = 5;
    Integer size = 15;
    List<String> facets = Arrays.asList("platform", "origin");

    SearchResult mockSemanticResult = createMockSearchResult(from, size, 10);
    when(mockSemanticEntitySearchService.search(
            any(), anyList(), anyString(), any(), any(), anyInt(), any()))
        .thenReturn(mockSemanticResult);

    SearchResult mockFacetResult = createMockFacetResult();
    when(mockCachingEntitySearchService.search(
            any(), anyList(), anyString(), any(), anyList(), eq(0), eq(0), anyList()))
        .thenReturn(mockFacetResult);

    when(mockEntityDocCountCache.getNonEmptyEntities(any()))
        .thenReturn(Arrays.asList("dataset", "chart"));

    // When
    SearchResult result =
        semanticSearchService.semanticSearchAcrossEntities(
            opContext, entityNames, query, null, null, from, size, facets);

    // Then
    assertNotNull(result);
    assertEquals(result.getFrom().intValue(), from);
    assertEquals(result.getPageSize().intValue(), size);
    assertEquals(result.getNumEntities().intValue(), 10);

    // Verify semantic service was called
    verify(mockSemanticEntitySearchService, times(1))
        .search(eq(opContext), eq(entityNames), eq(query), eq(null), eq(null), eq(from), eq(size));

    // Verify facet service was called for facets
    verify(mockCachingEntitySearchService, times(1))
        .search(any(), eq(entityNames), eq("*"), eq(null), anyList(), eq(0), eq(0), eq(facets));
  }

  @Test
  public void testSemanticSearchAcrossEntitiesWithoutFacets() {
    // Given
    List<String> entityNames = Collections.singletonList("dataset");
    String query = "test query";
    int from = 0;
    Integer size = 10;
    List<String> facets = Collections.emptyList(); // No facets requested

    SearchResult mockSemanticResult = createMockSearchResult(from, size, 3);
    when(mockSemanticEntitySearchService.search(
            any(), anyList(), anyString(), any(), any(), anyInt(), any()))
        .thenReturn(mockSemanticResult);

    when(mockEntityDocCountCache.getNonEmptyEntities(any()))
        .thenReturn(Collections.singletonList("dataset"));

    // When
    SearchResult result =
        semanticSearchService.semanticSearchAcrossEntities(
            opContext, entityNames, query, null, null, from, size, facets);

    // Then
    assertNotNull(result);
    assertEquals(result.getNumEntities().intValue(), 3);

    // Verify semantic service was called
    verify(mockSemanticEntitySearchService, times(1))
        .search(any(), anyList(), anyString(), any(), any(), anyInt(), any());

    // Verify facet service was NOT called (no facets requested)
    verify(mockCachingEntitySearchService, times(0))
        .search(any(), anyList(), anyString(), any(), anyList(), anyInt(), anyInt(), anyList());
  }

  @Test
  public void testSemanticSearchDisabled() {
    // Given: Semantic search disabled
    SearchServiceConfiguration disabledConfig =
        SearchServiceConfiguration.builder()
            .limit(
                LimitConfig.builder()
                    .results(ResultsLimitConfig.builder().apiDefault(100).max(1000).build())
                    .build())
            .semanticSearchEnabled(false)
            .build();

    SemanticSearchService disabledService =
        new SemanticSearchService(
            mockEntityDocCountCache,
            mockCachingEntitySearchService,
            mockSemanticEntitySearchService,
            disabledConfig);

    // When/Then: Should throw exception
    assertThrows(
        SemanticSearchDisabledException.class,
        () ->
            disabledService.semanticSearch(
                opContext, Collections.singletonList("dataset"), "query", null, null, 0, 10));
  }

  @Test
  public void testEmptyEntityListFallsBackToNonEmpty() {
    // Given: Empty entity list
    List<String> emptyEntityNames = Collections.emptyList();
    String query = "test";

    when(mockEntityDocCountCache.getNonEmptyEntities(any()))
        .thenReturn(Arrays.asList("dataset", "chart")); // Mock returns non-empty entities

    SearchResult mockResult = createMockSearchResult(0, 10, 2);
    when(mockSemanticEntitySearchService.search(
            any(), anyList(), anyString(), any(), any(), anyInt(), any()))
        .thenReturn(mockResult);

    // When
    SearchResult result =
        semanticSearchService.semanticSearch(opContext, emptyEntityNames, query, null, null, 0, 10);

    // Then
    assertNotNull(result);

    // Verify semantic service was called with non-empty entities from cache
    verify(mockSemanticEntitySearchService, times(1))
        .search(
            eq(opContext),
            eq(Arrays.asList("dataset", "chart")),
            eq(query),
            any(),
            any(),
            eq(0),
            eq(10));
  }

  @Test
  public void testEmptyEntityCacheReturnsEmptyResult() {
    // Given: Empty entity cache (no entities to search)
    List<String> entityNames = Collections.emptyList();
    String query = "test";

    when(mockEntityDocCountCache.getNonEmptyEntities(any()))
        .thenReturn(Collections.emptyList()); // No entities available

    // When
    SearchResult result =
        semanticSearchService.semanticSearch(opContext, entityNames, query, null, null, 0, 10);

    // Then: Should return empty result without calling semantic service
    assertNotNull(result);
    assertEquals(result.getNumEntities().intValue(), 0);
    assertEquals(result.getEntities().size(), 0);

    // Verify semantic service was NOT called
    verify(mockSemanticEntitySearchService, times(0))
        .search(any(), anyList(), anyString(), any(), any(), anyInt(), any());
  }

  private SearchResult createMockSearchResult(int from, int size, int numEntities) {
    SearchEntityArray entities = new SearchEntityArray();
    for (int i = 0; i < Math.min(numEntities, size); i++) {
      try {
        entities.add(
            new SearchEntity()
                .setEntity(
                    Urn.createFromString(
                        "urn:li:dataset:(urn:li:dataPlatform:test,entity" + i + ",PROD)"))
                .setScore(0.9 - i * 0.1));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return new SearchResult()
        .setEntities(entities)
        .setFrom(from)
        .setPageSize(size)
        .setNumEntities(numEntities)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  private SearchResult createMockFacetResult() {
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setFrom(0)
        .setPageSize(0)
        .setNumEntities(0)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }
}
