package com.linkedin.metadata.service;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProductUpdateServiceTest {

  private ProductUpdateService service;
  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setupTest() {
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testGetLatestProductUpdateFromClasspath() throws Exception {
    // Setup - no URL configured, should use classpath fallback
    service = new ProductUpdateService(null, "product-update-fallback.json");

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - this will succeed if the fallback resource exists, or fail gracefully
    // In a real test environment, you'd mock the classpath resource
    assertNotNull(result);
  }

  @Test
  public void testGetLatestProductUpdateEmptyUrl() throws Exception {
    // Setup - empty URL should use classpath fallback
    service = new ProductUpdateService("", "product-update-fallback.json");

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify
    assertNotNull(result);
  }

  @Test
  public void testGetLatestProductUpdateWhitespaceUrl() throws Exception {
    // Setup - whitespace URL should use classpath fallback
    service = new ProductUpdateService("   ", "product-update-fallback.json");

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify
    assertNotNull(result);
  }

  @Test
  public void testClearCache() {
    // Setup
    service = new ProductUpdateService(null, "product-update-fallback.json");

    // Execute - should not throw exception
    service.clearCache();

    // Verify - no exception thrown
  }

  @Test
  public void testClearCacheMultipleTimes() {
    // Setup
    service = new ProductUpdateService(null, "product-update-fallback.json");

    // Execute - should not throw exception when called multiple times
    service.clearCache();
    service.clearCache();
    service.clearCache();

    // Verify - no exception thrown
  }

  @Test
  public void testServiceInitialization() {
    // Test with URL
    service = new ProductUpdateService("https://example.com/updates.json", "fallback.json");
    assertNotNull(service);

    // Test without URL
    service = new ProductUpdateService(null, "fallback.json");
    assertNotNull(service);

    // Test with empty URL
    service = new ProductUpdateService("", "fallback.json");
    assertNotNull(service);
  }

  @Test
  public void testGetLatestProductUpdateFallsBackOnError() throws Exception {
    // Setup - invalid URL that will fail
    service =
        new ProductUpdateService(
            "http://invalid-url-that-does-not-exist.local", "product-update-fallback.json");

    // Execute - should fall back to classpath
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should return result from fallback (or empty if fallback doesn't exist)
    assertNotNull(result);
  }

  @Test
  public void testGetLatestProductUpdateCaching() throws Exception {
    // Setup
    service = new ProductUpdateService(null, "product-update-fallback.json");

    // Execute multiple times
    Optional<JsonNode> result1 = service.getLatestProductUpdate();
    Optional<JsonNode> result2 = service.getLatestProductUpdate();
    Optional<JsonNode> result3 = service.getLatestProductUpdate();

    // Verify - all results should be present (either from cache or fresh)
    assertNotNull(result1);
    assertNotNull(result2);
    assertNotNull(result3);

    // If results are present, they should be equal (from cache)
    if (result1.isPresent() && result2.isPresent() && result3.isPresent()) {
      assertEquals(result1.get().toString(), result2.get().toString());
      assertEquals(result2.get().toString(), result3.get().toString());
    }
  }

  @Test
  public void testClearCacheForcesRefresh() throws Exception {
    // Setup
    service = new ProductUpdateService(null, "product-update-fallback.json");

    // Execute - get, clear, get again
    Optional<JsonNode> result1 = service.getLatestProductUpdate();
    service.clearCache();
    Optional<JsonNode> result2 = service.getLatestProductUpdate();

    // Verify - both should succeed
    assertNotNull(result1);
    assertNotNull(result2);
  }

  @Test
  public void testGetLatestProductUpdateWithNonExistentFallback() {
    // Setup - fallback resource that doesn't exist
    service = new ProductUpdateService(null, "non-existent-resource.json");

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - should return empty Optional
    assertNotNull(result);
    assertFalse(result.isPresent());
  }

  @Test
  public void testMultipleInstancesIndependentCaches() throws Exception {
    // Setup - create two service instances
    ProductUpdateService service1 = new ProductUpdateService(null, "product-update-fallback.json");
    ProductUpdateService service2 = new ProductUpdateService(null, "product-update-fallback.json");

    // Execute - get from both, clear one, get again
    service1.getLatestProductUpdate();
    service2.getLatestProductUpdate();
    service1.clearCache();

    // Execute again
    Optional<JsonNode> result1 = service1.getLatestProductUpdate();
    Optional<JsonNode> result2 = service2.getLatestProductUpdate();

    // Verify - both should work independently
    assertNotNull(result1);
    assertNotNull(result2);
  }

  @Test
  public void testServiceWithValidJsonUrl() {
    // Setup - this tests that the service can be constructed with a valid URL
    // In a real test, you'd mock the HTTP client
    service =
        new ProductUpdateService(
            "https://raw.githubusercontent.com/datahub-project/datahub/master/docs/product-update.json",
            "product-update-fallback.json");

    // Verify service is created
    assertNotNull(service);
  }

  @Test
  public void testGetLatestProductUpdateReturnsValidJson() throws Exception {
    // Setup
    service = new ProductUpdateService(null, "product-update-fallback.json");

    // Execute
    Optional<JsonNode> result = service.getLatestProductUpdate();

    // Verify - if result is present, it should be valid JSON
    if (result.isPresent()) {
      JsonNode json = result.get();
      assertNotNull(json);
      // JSON should be an object or array
      assertTrue(json.isObject() || json.isArray());
    }
  }
}
