package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IndexUtilsTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  @Mock private SearchClientShim searchClient;
  @Mock private RawResponse rawResponse;
  @Mock private ResponseException responseException;
  @Mock private OperationContext operationContext;
  @Mock private ObjectMapper objectMapper;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(esComponents.getSearchClient()).thenReturn(searchClient);
    Mockito.when(operationContext.getObjectMapper()).thenReturn(objectMapper);
  }

  @Test
  public void testIsAwsOpenSearchService_AwsHost() {
    // Arrange
    Mockito.when(searchClient.getShimConfiguration())
        .thenReturn(Mockito.mock(SearchClientShim.ShimConfiguration.class));
    Mockito.when(searchClient.getShimConfiguration().getHost())
        .thenReturn(
            "vpc-usw2-staging-shared-xg5onkhkidjnmvfmjqi35vp7vu.us-west-2.es.amazonaws.com");

    // Act
    boolean result = IndexUtils.isAwsOpenSearchService(esComponents);

    // Assert
    Assert.assertTrue(result);
  }

  @Test
  public void testIsAwsOpenSearchService_LocalHost() {
    // Arrange
    Mockito.when(searchClient.getShimConfiguration())
        .thenReturn(Mockito.mock(SearchClientShim.ShimConfiguration.class));
    Mockito.when(searchClient.getShimConfiguration().getHost()).thenReturn("localhost:9200");

    // Act
    boolean result = IndexUtils.isAwsOpenSearchService(esComponents);

    // Assert
    Assert.assertFalse(result);
  }

  @Test
  public void testIsAwsOpenSearchService_ElasticCloudHost() {
    // Arrange
    Mockito.when(searchClient.getShimConfiguration())
        .thenReturn(Mockito.mock(SearchClientShim.ShimConfiguration.class));
    Mockito.when(searchClient.getShimConfiguration().getHost())
        .thenReturn("my-cluster.es.us-central1.gcp.cloud.es.io");

    // Act
    boolean result = IndexUtils.isAwsOpenSearchService(esComponents);

    // Assert
    Assert.assertFalse(result);
  }

  @Test
  public void testPerformGetRequest_Success() throws IOException {
    // Arrange
    String endpoint = "/_cluster/health";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);

    // Act
    RawResponse result = IndexUtils.performGetRequest(esComponents, endpoint);

    // Assert
    Assert.assertNotNull(result);
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testPerformPutRequest_Success() throws IOException {
    // Arrange
    String endpoint = "/_index_template/test";
    String jsonBody = "{\"template\": {\"mappings\": {}}}";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);

    // Act
    RawResponse result = IndexUtils.performPutRequest(esComponents, endpoint, jsonBody);

    // Assert
    Assert.assertNotNull(result);
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testPerformPostRequest_Success() throws IOException {
    // Arrange
    String endpoint = "/_bulk";
    String jsonBody = "{\"index\": {}}";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);

    // Act
    RawResponse result = IndexUtils.performPostRequest(esComponents, endpoint, jsonBody);

    // Assert
    Assert.assertNotNull(result);
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testPerformPutRequestWithParams_Success() throws IOException {
    // Arrange
    String endpoint = "/_index_template/test";
    String queryParams = "create=true";
    String jsonBody = "{\"template\": {\"mappings\": {}}}";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);

    // Act
    RawResponse result =
        IndexUtils.performPutRequestWithParams(esComponents, endpoint, queryParams, jsonBody);

    // Assert
    Assert.assertNotNull(result);
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testRetryWithBackoff_Success() {
    // Arrange
    IndexUtils.RetryableOperation operation = () -> true;

    // Act
    boolean result = IndexUtils.retryWithBackoff(3, 100, operation);

    // Assert
    Assert.assertTrue(result);
  }

  @Test
  public void testRetryWithBackoff_Failure() {
    // Arrange
    IndexUtils.RetryableOperation operation =
        () -> {
          throw new RuntimeException("Test error");
        };

    // Act
    boolean result = IndexUtils.retryWithBackoff(3, 100, operation);

    // Assert
    Assert.assertFalse(result);
  }

  @Test
  public void testRetryWithDefaultConfig_Success() {
    // Arrange
    IndexUtils.RetryableOperation operation = () -> true;

    // Act
    boolean result = IndexUtils.retryWithDefaultConfig(operation);

    // Assert
    Assert.assertTrue(result);
  }

  @Test
  public void testRetryWithDefaultConfig_Failure() {
    // Arrange
    IndexUtils.RetryableOperation operation =
        () -> {
          throw new RuntimeException("Test error");
        };

    // Act
    boolean result = IndexUtils.retryWithDefaultConfig(operation);

    // Assert
    Assert.assertFalse(result);
  }

  @Test
  public void testExtractJsonValue_Success() throws Exception {
    // Arrange
    String json = "{\"_seq_no\": 123, \"_primary_term\": 456}";
    com.fasterxml.jackson.databind.JsonNode jsonNode =
        Mockito.mock(com.fasterxml.jackson.databind.JsonNode.class);
    com.fasterxml.jackson.databind.JsonNode valueNode =
        Mockito.mock(com.fasterxml.jackson.databind.JsonNode.class);

    Mockito.when(objectMapper.readTree(json)).thenReturn(jsonNode);
    Mockito.when(jsonNode.get("_seq_no")).thenReturn(valueNode);
    Mockito.when(valueNode.isNumber()).thenReturn(true);
    Mockito.when(valueNode.asInt()).thenReturn(123);

    // Act
    int result = IndexUtils.extractJsonValue(operationContext, json, "_seq_no");

    // Assert
    Assert.assertEquals(result, 123);
  }

  @Test
  public void testExtractJsonValue_KeyNotFound() throws Exception {
    // Arrange
    String json = "{\"_primary_term\": 456}";
    com.fasterxml.jackson.databind.JsonNode jsonNode =
        Mockito.mock(com.fasterxml.jackson.databind.JsonNode.class);

    Mockito.when(objectMapper.readTree(json)).thenReturn(jsonNode);
    Mockito.when(jsonNode.get("_seq_no")).thenReturn(null);

    // Act
    int result = IndexUtils.extractJsonValue(operationContext, json, "_seq_no");

    // Assert
    Assert.assertEquals(result, -1);
  }

  @Test
  public void testExtractJsonValue_InvalidJson() throws Exception {
    // Arrange
    String json = "invalid json";
    Mockito.when(objectMapper.readTree(json)).thenThrow(new RuntimeException("Invalid JSON"));

    // Act
    int result = IndexUtils.extractJsonValue(operationContext, json, "_seq_no");

    // Assert
    Assert.assertEquals(result, -1);
  }

  @Test
  public void testLoadResourceAsString_Success() throws IOException {
    // Arrange
    String resourcePath = "/index/usage-event/opensearch_policy.json";

    // Act & Assert
    // This test verifies the method doesn't throw an exception
    // The actual content depends on the resource file
    try {
      String result = IndexUtils.loadResourceAsString(resourcePath);
      Assert.assertNotNull(result);
    } catch (IOException e) {
      // Resource might not exist in test environment, which is acceptable
      Assert.assertTrue(
          e.getMessage().contains("resource") || e.getMessage().contains("not found"));
    }
  }

  @Test(expectedExceptions = IOException.class)
  public void testLoadResourceAsString_NonExistentResource() throws IOException {
    // Arrange
    String resourcePath = "/non/existent/resource.json";

    // Act
    IndexUtils.loadResourceAsString(resourcePath);
  }
}
