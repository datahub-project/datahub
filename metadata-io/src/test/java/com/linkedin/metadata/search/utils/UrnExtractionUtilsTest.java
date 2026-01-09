package com.linkedin.metadata.search.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UrnExtractionUtilsTest {

  @Mock private SearchHit mockSearchHit;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testExtractUrnFromSearchHit_ValidUrn() {
    // Given
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);

    // When
    Urn result = UrnExtractionUtils.extractUrnFromSearchHit(mockSearchHit);

    // Then
    Assert.assertNotNull(result);
    Assert.assertEquals(
        result.toString(), "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testExtractUrnFromSearchHit_NullUrn() {
    // Given
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", null);
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);

    // When/Then - should throw RuntimeException
    UrnExtractionUtils.extractUrnFromSearchHit(mockSearchHit);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testExtractUrnFromSearchHit_InvalidUrn() {
    // Given
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", "invalid:urn:format");
    when(mockSearchHit.getSourceAsMap()).thenReturn(sourceMap);

    // When/Then - should throw RuntimeException
    UrnExtractionUtils.extractUrnFromSearchHit(mockSearchHit);
  }

  @Test
  public void testExtractUrnFromNestedFieldSafely_ValidUrn() {
    // Given
    Map<String, Object> document = new HashMap<>();
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");
    document.put("source", sourceMap);

    // When
    Urn result = UrnExtractionUtils.extractUrnFromNestedFieldSafely(document, "source", "source");

    // Then
    Assert.assertNotNull(result);
    Assert.assertEquals(
        result.toString(), "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");
  }

  @Test
  public void testExtractUrnFromNestedFieldSafely_NullNestedField() {
    // Given
    Map<String, Object> document = new HashMap<>();
    document.put("source", null);

    // When
    Urn result = UrnExtractionUtils.extractUrnFromNestedFieldSafely(document, "source", "source");

    // Then
    Assert.assertNull(result);
  }

  @Test
  public void testExtractUrnFromNestedFieldSafely_NullUrn() {
    // Given
    Map<String, Object> document = new HashMap<>();
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", null);
    document.put("source", sourceMap);

    // When
    Urn result = UrnExtractionUtils.extractUrnFromNestedFieldSafely(document, "source", "source");

    // Then
    Assert.assertNull(result);
  }

  @Test
  public void testExtractUrnFromNestedFieldSafely_InvalidUrn() {
    // Given
    Map<String, Object> document = new HashMap<>();
    Map<String, Object> sourceMap = new HashMap<>();
    sourceMap.put("urn", "invalid:urn:format");
    document.put("source", sourceMap);

    // When
    Urn result = UrnExtractionUtils.extractUrnFromNestedFieldSafely(document, "source", "source");

    // Then
    Assert.assertNull(result);
  }

  // ============================================================================
  // extractUniqueUrns Tests
  // ============================================================================

  @Test
  public void testExtractUniqueUrns_ValidUrns() {
    // Given
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit hit1 = mock(SearchHit.class);
    SearchHit hit2 = mock(SearchHit.class);

    Map<String, Object> sourceMap1 = new HashMap<>();
    sourceMap1.put("urn", "urn:li:assertion:test-1");
    Map<String, Object> sourceMap2 = new HashMap<>();
    sourceMap2.put("urn", "urn:li:assertion:test-2");

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {hit1, hit2});
    when(hit1.getSourceAsMap()).thenReturn(sourceMap1);
    when(hit2.getSourceAsMap()).thenReturn(sourceMap2);

    // When
    Set<Urn> result = UrnExtractionUtils.extractUniqueUrns(mockResponse);

    // Then
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testExtractUniqueUrns_EmptyResponse() {
    // Given
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[0]);

    // When
    Set<Urn> result = UrnExtractionUtils.extractUniqueUrns(mockResponse);

    // Then
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractUniqueUrns_NullHits() {
    // Given
    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockResponse.getHits()).thenReturn(null);

    // When
    Set<Urn> result = UrnExtractionUtils.extractUniqueUrns(mockResponse);

    // Then
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractUniqueUrns_SkipsInvalidUrns() {
    // Given
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit validHit = mock(SearchHit.class);
    SearchHit invalidHit = mock(SearchHit.class);

    Map<String, Object> validSourceMap = new HashMap<>();
    validSourceMap.put("urn", "urn:li:assertion:valid");
    Map<String, Object> invalidSourceMap = new HashMap<>();
    invalidSourceMap.put("urn", null); // Invalid - null URN

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {validHit, invalidHit});
    when(validHit.getSourceAsMap()).thenReturn(validSourceMap);
    when(invalidHit.getSourceAsMap()).thenReturn(invalidSourceMap);

    // When
    Set<Urn> result = UrnExtractionUtils.extractUniqueUrns(mockResponse);

    // Then - should only have the valid URN
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testExtractUniqueUrns_DeduplicatesUrns() {
    // Given
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit hit1 = mock(SearchHit.class);
    SearchHit hit2 = mock(SearchHit.class);

    // Same URN in both hits (e.g., system metadata with multiple aspects)
    Map<String, Object> sourceMap1 = new HashMap<>();
    sourceMap1.put("urn", "urn:li:assertion:same-urn");
    Map<String, Object> sourceMap2 = new HashMap<>();
    sourceMap2.put("urn", "urn:li:assertion:same-urn");

    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockHits.getHits()).thenReturn(new SearchHit[] {hit1, hit2});
    when(hit1.getSourceAsMap()).thenReturn(sourceMap1);
    when(hit2.getSourceAsMap()).thenReturn(sourceMap2);

    // When
    Set<Urn> result = UrnExtractionUtils.extractUniqueUrns(mockResponse);

    // Then - should deduplicate
    Assert.assertEquals(result.size(), 1);
  }
}
