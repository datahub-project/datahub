package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for ProductUpdateService focusing on HTTP fetching, caching behavior, and fallback logic.
 */
public class ProductUpdateServiceTest {

  private static final String TEST_RESOURCE_PATH = "product-update-for-test.json";
  private static final String TEST_URL = "https://example.com/product-update.json";

  private static final String MOCK_PRODUCT_JSON_V1 =
      "{"
          + "\"enabled\": true,"
          + "\"id\": \"remote-v1\","
          + "\"title\": \"Remote Update V1\","
          + "\"description\": \"First remote version\""
          + "}";

  private static final String MOCK_PRODUCT_JSON_V2 =
      "{"
          + "\"enabled\": true,"
          + "\"id\": \"remote-v2\","
          + "\"title\": \"Remote Update V2\","
          + "\"description\": \"Second remote version\""
          + "}";

  @Mock private HttpClient mockHttpClient;
  @Mock private HttpResponse<String> mockHttpResponse;

  private ProductUpdateService service;
  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setupTest() {
    MockitoAnnotations.openMocks(this);
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testSuccessfulRemoteFetchIsCached() throws Exception {
    // Setup - mock successful HTTP response
    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(MOCK_PRODUCT_JSON_V1);

    service = new ProductUpdateService(TEST_URL, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute - fetch multiple times
    Optional<JsonNode> result1 = service.getLatestProductUpdate();
    Optional<JsonNode> result2 = service.getLatestProductUpdate();
    Optional<JsonNode> result3 = service.getLatestProductUpdate();

    // Verify - HTTP should only be called once (cached after first fetch)
    verify(mockHttpClient, times(1)).<String>send(any(HttpRequest.class), any());

    // All results should be the remote version
    assertTrue(result1.isPresent());
    assertTrue(result2.isPresent());
    assertTrue(result3.isPresent());
    assertEquals(result1.get().get("id").asText(), "remote-v1");
    assertEquals(result2.get().get("id").asText(), "remote-v1");
    assertEquals(result3.get().get("id").asText(), "remote-v1");
  }

  @Test
  public void testClearCacheRefetchesFromRemote() throws Exception {
    // Setup - mock HTTP to return V1 first, then V2
    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(MOCK_PRODUCT_JSON_V1).thenReturn(MOCK_PRODUCT_JSON_V2);

    service = new ProductUpdateService(TEST_URL, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute - fetch, clear cache, fetch again
    Optional<JsonNode> result1 = service.getLatestProductUpdate();
    service.clearCache();
    Optional<JsonNode> result2 = service.getLatestProductUpdate();

    // Verify - HTTP should be called twice (once before clear, once after)
    verify(mockHttpClient, times(2)).<String>send(any(HttpRequest.class), any());

    // Results should reflect the different versions
    assertTrue(result1.isPresent());
    assertTrue(result2.isPresent());
    assertEquals(result1.get().get("id").asText(), "remote-v1");
    assertEquals(result2.get().get("id").asText(), "remote-v2");
  }

  @Test
  public void testRemoteFetchFailsFallsBackToClasspath() throws Exception {
    // Setup - mock HTTP failure
    when(mockHttpClient.<String>send(any(HttpRequest.class), any()))
        .thenThrow(new IOException("Connection failed"));

    service = new ProductUpdateService(TEST_URL, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should fall back to classpath resource
    assertTrue(result.isPresent());
    assertEquals(result.get().get("id").asText(), "test-product-update");
  }

  @Test
  public void testRemoteReturnsNon200FallsBackToClasspath() throws Exception {
    // Setup - mock HTTP 404 response
    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(404);

    service = new ProductUpdateService(TEST_URL, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should fall back to classpath resource
    assertTrue(result.isPresent());
    assertEquals(result.get().get("id").asText(), "test-product-update");
  }

  @Test
  public void testRemoteReturnsEmptyBodyFallsBackToClasspath() throws Exception {
    // Setup - mock empty response body
    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn("");

    service = new ProductUpdateService(TEST_URL, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should fall back to classpath resource
    assertTrue(result.isPresent());
    assertEquals(result.get().get("id").asText(), "test-product-update");
  }

  @Test
  public void testAfterRemoteFailsClearCacheRetries() throws Exception {
    // Setup - mock HTTP to fail first, then succeed
    when(mockHttpClient.<String>send(any(HttpRequest.class), any()))
        .thenThrow(new IOException("Connection failed"))
        .thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(MOCK_PRODUCT_JSON_V1);

    service = new ProductUpdateService(TEST_URL, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute - first fetch fails and falls back, clear cache, fetch again succeeds
    Optional<JsonNode> result1 = service.getLatestProductUpdate();
    service.clearCache();
    Optional<JsonNode> result2 = service.getLatestProductUpdate();

    // Verify - HTTP should be attempted twice
    verify(mockHttpClient, times(2)).<String>send(any(HttpRequest.class), any());

    // First result should be fallback, second should be remote
    assertTrue(result1.isPresent());
    assertTrue(result2.isPresent());
    assertEquals(result1.get().get("id").asText(), "test-product-update");
    assertEquals(result2.get().get("id").asText(), "remote-v1");
  }

  @Test
  public void testNoUrlConfiguredUsesFallback() throws Exception {
    // Setup - no URL configured
    service = new ProductUpdateService(null, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should not attempt HTTP, should use fallback
    verify(mockHttpClient, never()).<String>send(any(HttpRequest.class), any());
    assertTrue(result.isPresent());
    assertEquals(result.get().get("id").asText(), "test-product-update");
  }

  @Test
  public void testEmptyUrlUsesFallback() throws Exception {
    // Setup - empty URL
    service = new ProductUpdateService("", TEST_RESOURCE_PATH, mockHttpClient);

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should not attempt HTTP, should use fallback
    verify(mockHttpClient, never()).<String>send(any(HttpRequest.class), any());
    assertTrue(result.isPresent());
    assertEquals(result.get().get("id").asText(), "test-product-update");
  }

  @Test
  public void testWhitespaceUrlUsesFallback() throws Exception {
    // Setup - whitespace URL
    service = new ProductUpdateService("   ", TEST_RESOURCE_PATH, mockHttpClient);

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should not attempt HTTP, should use fallback
    verify(mockHttpClient, never()).<String>send(any(HttpRequest.class), any());
    assertTrue(result.isPresent());
    assertEquals(result.get().get("id").asText(), "test-product-update");
  }

  @Test
  public void testNonExistentFallbackReturnsEmpty() throws Exception {
    // Setup - no URL and non-existent fallback
    service = new ProductUpdateService(null, "non-existent-resource.json", mockHttpClient);

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should return empty when fallback doesn't exist
    assertNotNull(result);
    assertFalse(result.isPresent());
  }

  @Test
  public void testMultipleInstancesHaveIndependentCaches() throws Exception {
    // Setup - two service instances with mocked responses
    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(MOCK_PRODUCT_JSON_V1);

    ProductUpdateService service1 =
        new ProductUpdateService(TEST_URL, TEST_RESOURCE_PATH, mockHttpClient);
    ProductUpdateService service2 =
        new ProductUpdateService(TEST_URL, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute - fetch from both, clear one, fetch again
    service1.getLatestProductUpdate();
    service2.getLatestProductUpdate();
    service1.clearCache();
    service1.getLatestProductUpdate();
    service2.getLatestProductUpdate();

    // Verify - service1 should fetch twice (initial + after clear), service2 only once (cached)
    // Total HTTP calls: 3 (1 from service1 initial, 1 from service2 initial, 1 from service1 after
    // clear)
    verify(mockHttpClient, times(3)).<String>send(any(HttpRequest.class), any());
  }

  @Test
  public void testFallbackJsonStructure() throws Exception {
    // Setup
    service = new ProductUpdateService(null, TEST_RESOURCE_PATH, mockHttpClient);

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - validate expected JSON structure from test resource
    assertTrue(result.isPresent());
    JsonNode json = result.get();

    assertTrue(json.isObject());
    assertTrue(json.has("id"));
    assertTrue(json.has("enabled"));
    assertTrue(json.has("title"));
    assertTrue(json.has("description"));

    assertEquals(json.get("id").asText(), "test-product-update");
    assertTrue(json.get("enabled").asBoolean());
    assertEquals(json.get("title").asText(), "Test Product Update");
  }
}
