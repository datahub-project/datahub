package com.linkedin.metadata.search.utils;

import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.search.SearchHit;
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
}
