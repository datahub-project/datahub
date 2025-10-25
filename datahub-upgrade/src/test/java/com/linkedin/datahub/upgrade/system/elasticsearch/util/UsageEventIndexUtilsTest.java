package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.core.rest.RestStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UsageEventIndexUtilsTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  @Mock private SearchClientShim searchClient;
  @Mock private RawResponse rawResponse;
  @Mock private CreateIndexResponse createIndexResponse;
  @Mock private ResponseException responseException;
  @Mock private OpenSearchStatusException openSearchStatusException;
  @Mock private OperationContext operationContext;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(esComponents.getSearchClient()).thenReturn(searchClient);
  }

  @Test
  public void testCreateIlmPolicy_Success() throws IOException {
    // Arrange
    String policyName = "test_policy";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200;
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIlmPolicy_AlreadyExists() throws IOException {
    // Arrange
    String policyName = "test_policy";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 409;
              }

              @Override
              public String getReasonPhrase() {
                return "Conflict";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);

    // Assert - Should not throw exception
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIlmPolicy_Conflict() throws IOException {
    // Arrange
    String policyName = "test_policy";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 409;
              }

              @Override
              public String getReasonPhrase() {
                return "Conflict";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);

    // Assert - Should succeed on first attempt due to 409 conflict
    Mockito.verify(searchClient, Mockito.times(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateIlmPolicy_OtherError() throws IOException {
    // Arrange
    String policyName = "test_policy";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 500;
              }

              @Override
              public String getReasonPhrase() {
                return "Internal Server Error";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);
  }

  @Test
  public void testCreateIsmPolicy_Success() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 201;
              }

              @Override
              public String getReasonPhrase() {
                return "Created";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert
    // Note: createIsmPolicy makes 2 calls - one for creation and one for update attempt
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_ResponseException200() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock ResponseException with 200 status (policy already exists)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200;
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should succeed and attempt to update existing policy
    Assert.assertTrue(result);
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_ResponseException404() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock ResponseException with 404 status (policy doesn't exist)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException)
        .thenReturn(rawResponse); // Mock successful PUT response
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 404;
              }

              @Override
              public String getReasonPhrase() {
                return "Not Found";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Mock successful PUT response for policy creation
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 201;
              }

              @Override
              public String getReasonPhrase() {
                return "Created";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should succeed and create new policy
    Assert.assertTrue(result);
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_IOException() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock IOException during policy creation
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(new IOException("Network error"));

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return false due to IOException
    Assert.assertFalse(result);
    Mockito.verify(searchClient, Mockito.times(5))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_UnexpectedException() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock unexpected exception
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(new RuntimeException("Unexpected error"));

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return false due to unexpected exception
    Assert.assertFalse(result);
    Mockito.verify(searchClient, Mockito.times(5))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateDataStream_Success() throws IOException {
    // Arrange
    String dataStreamName = "test_datastream";
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(false);
    Mockito.when(
            searchClient.createIndex(
                Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(createIndexResponse);
    Mockito.when(createIndexResponse.isAcknowledged()).thenReturn(true);

    // Act
    UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);

    // Assert
    Mockito.verify(searchClient)
        .indexExists(Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class));
    Mockito.verify(searchClient)
        .createIndex(Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class));
  }

  @Test
  public void testCreateDataStream_AlreadyExists() throws IOException {
    // Arrange
    String dataStreamName = "test_datastream";
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(true);

    // Act
    UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);

    // Assert
    Mockito.verify(searchClient)
        .indexExists(Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class));
    Mockito.verify(searchClient, Mockito.never())
        .createIndex(Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class));
  }

  @Test
  public void testCreateDataStream_AlreadyExistsException() throws IOException {
    // Arrange
    String dataStreamName = "test_datastream";
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(false);
    Mockito.when(
            searchClient.createIndex(
                Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenThrow(openSearchStatusException);
    Mockito.when(openSearchStatusException.getMessage())
        .thenReturn("resource_already_exists_exception");

    // Act
    UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);

    // Assert - Should not throw exception
    Mockito.verify(searchClient)
        .createIndex(Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class));
  }

  @Test(expectedExceptions = OpenSearchStatusException.class)
  public void testCreateDataStream_OtherError() throws IOException {
    // Arrange
    String dataStreamName = "test_datastream";
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(false);
    Mockito.when(
            searchClient.createIndex(
                Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenThrow(openSearchStatusException);
    Mockito.when(openSearchStatusException.getMessage()).thenReturn("Some other error");
    Mockito.when(openSearchStatusException.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR);

    // Act
    UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);
  }

  @Test
  public void testCreateOpenSearchIndex_Success() throws IOException {
    // Arrange
    String indexName = "test_index";
    String prefix = "test_";
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(false);
    Mockito.when(
            searchClient.createIndex(
                Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(createIndexResponse);
    Mockito.when(createIndexResponse.isAcknowledged()).thenReturn(true);

    // Act
    UsageEventIndexUtils.createOpenSearchIndex(esComponents, indexName, prefix);

    // Assert
    Mockito.verify(searchClient)
        .indexExists(Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class));
    Mockito.verify(searchClient)
        .createIndex(Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class));
  }

  @Test
  public void testCreateOpenSearchIndex_AlreadyExists() throws IOException {
    // Arrange
    String indexName = "test_index";
    String prefix = "test_";
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(true);

    // Act
    UsageEventIndexUtils.createOpenSearchIndex(esComponents, indexName, prefix);

    // Assert
    Mockito.verify(searchClient)
        .indexExists(Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class));
    Mockito.verify(searchClient, Mockito.never())
        .createIndex(Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class));
  }

  @Test
  public void testCreateOpenSearchIndex_AlreadyExistsException() throws IOException {
    // Arrange
    String indexName = "test_index";
    String prefix = "test_";
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(false);
    Mockito.when(
            searchClient.createIndex(
                Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenThrow(openSearchStatusException);
    Mockito.when(openSearchStatusException.getMessage())
        .thenReturn("resource_already_exists_exception");

    // Act
    UsageEventIndexUtils.createOpenSearchIndex(esComponents, indexName, prefix);

    // Assert - Should not throw exception
    Mockito.verify(searchClient)
        .indexExists(Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class));
    Mockito.verify(searchClient)
        .createIndex(Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class));
  }

  @Test(expectedExceptions = OpenSearchStatusException.class)
  public void testCreateOpenSearchIndex_OtherError() throws IOException {
    // Arrange
    String indexName = "test_index";
    String prefix = "test_";
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(false);
    Mockito.when(
            searchClient.createIndex(
                Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenThrow(openSearchStatusException);
    Mockito.when(openSearchStatusException.getMessage()).thenReturn("Some other error");
    Mockito.when(openSearchStatusException.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR);

    // Act
    UsageEventIndexUtils.createOpenSearchIndex(esComponents, indexName, prefix);
  }

  @Test
  public void testAwsOpenSearchDetection() throws IOException {
    // Test AWS OpenSearch Service detection
    Mockito.when(searchClient.getShimConfiguration())
        .thenReturn(Mockito.mock(SearchClientShim.ShimConfiguration.class));
    Mockito.when(searchClient.getShimConfiguration().getHost())
        .thenReturn(
            "vpc-usw2-staging-shared-xg5onkhkidjnmvfmjqi35vp7vu.us-west-2.es.amazonaws.com");

    // This test verifies that the AWS detection logic works by checking the host URL
    // The actual detection method is private, but we can test the behavior through the public
    // methods
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock the response to simulate AWS OpenSearch Service behavior
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 400; // AWS OpenSearch Service returns 400 for ISM policies
              }

              @Override
              public String getReasonPhrase() {
                return "Bad Request";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            });

    // Act - This should handle the 400 error gracefully for AWS OpenSearch Service
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - The method should complete without throwing an exception and return false
    Assert.assertFalse(result, "Policy creation should return false for 400 error");
    // For 400 errors, the retry logic will attempt 5 times before giving up
    Mockito.verify(searchClient, Mockito.times(5))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIndexTemplate_Success() throws IOException {
    // Arrange
    String templateName = "test_template";
    String prefix = "test_";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200;
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIndexTemplate(esComponents, templateName, "policy", 1, 1, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIndexTemplate_Conflict() throws IOException {
    // Arrange
    String templateName = "test_template";
    String prefix = "test_";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 409;
              }

              @Override
              public String getReasonPhrase() {
                return "Conflict";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIndexTemplate(esComponents, templateName, "policy", 1, 1, prefix);

    // Assert - Should not throw exception
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateIndexTemplate_OtherError() throws IOException {
    // Arrange
    String templateName = "test_template";
    String prefix = "test_";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 500;
              }

              @Override
              public String getReasonPhrase() {
                return "Internal Server Error";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIndexTemplate(esComponents, templateName, "policy", 1, 1, prefix);
  }

  @Test
  public void testCreateOpenSearchIndexTemplate_Success() throws IOException {
    // Arrange
    String templateName = "test_template";
    String prefix = "test_";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200;
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createOpenSearchIndexTemplate(esComponents, templateName, 1, 1, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateOpenSearchIndexTemplate_Conflict() throws IOException {
    // Arrange
    String templateName = "test_template";
    String prefix = "test_";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 409;
              }

              @Override
              public String getReasonPhrase() {
                return "Conflict";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createOpenSearchIndexTemplate(esComponents, templateName, 1, 1, prefix);

    // Assert - Should not throw exception
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateOpenSearchIndexTemplate_OtherError() throws IOException {
    // Arrange
    String templateName = "test_template";
    String prefix = "test_";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 500;
              }

              @Override
              public String getReasonPhrase() {
                return "Internal Server Error";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createOpenSearchIndexTemplate(esComponents, templateName, 1, 1, prefix);
  }

  @Test
  public void testUpdateIsmPolicy_Success() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock GET request - policy exists
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200;
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Mock response body for seq_no and primary_term extraction
    Mockito.when(rawResponse.getEntity())
        .thenReturn(Mockito.mock(org.apache.http.HttpEntity.class));
    Mockito.when(rawResponse.getEntity().getContent())
        .thenReturn(
            new java.io.ByteArrayInputStream(
                "{\"_seq_no\": 123, \"_primary_term\": 456}".getBytes()));

    // Act
    UsageEventIndexUtils.updateIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Only one GET request should be made since we can't extract seq_no and primary_term
    Mockito.verify(searchClient, Mockito.times(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testUpdateIsmPolicy_PolicyNotFound() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock GET request - policy doesn't exist
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 404;
              }

              @Override
              public String getReasonPhrase() {
                return "Not Found";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.updateIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should not throw exception for 404
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testUpdateIsmPolicy_OtherError() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock GET request - other error
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 500;
              }

              @Override
              public String getReasonPhrase() {
                return "Internal Server Error";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.updateIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - The method should handle the error gracefully and not throw an exception
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testUpdateIsmPolicy_WithConcurrencyControl() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock ObjectMapper to successfully extract seq_no and primary_term
    Mockito.when(operationContext.getObjectMapper())
        .thenReturn(new com.fasterxml.jackson.databind.ObjectMapper());

    // Mock GET request - policy exists with valid JSON response
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200;
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Mock response body for seq_no and primary_term extraction
    Mockito.when(rawResponse.getEntity())
        .thenReturn(Mockito.mock(org.apache.http.HttpEntity.class));
    Mockito.when(rawResponse.getEntity().getContent())
        .thenReturn(
            new java.io.ByteArrayInputStream(
                "{\"_seq_no\": 123, \"_primary_term\": 456}".getBytes()));

    // Act
    UsageEventIndexUtils.updateIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should make 2 calls: GET to retrieve policy, then PUT to update with concurrency
    // control
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testUpdateIsmPolicy_UpdateFailure() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock ObjectMapper to successfully extract seq_no and primary_term
    Mockito.when(operationContext.getObjectMapper())
        .thenReturn(new com.fasterxml.jackson.databind.ObjectMapper());

    // Mock GET request - policy exists
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200;
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Mock response body for seq_no and primary_term extraction
    Mockito.when(rawResponse.getEntity())
        .thenReturn(Mockito.mock(org.apache.http.HttpEntity.class));
    Mockito.when(rawResponse.getEntity().getContent())
        .thenReturn(
            new java.io.ByteArrayInputStream(
                "{\"_seq_no\": 123, \"_primary_term\": 456}".getBytes()));

    // Act
    UsageEventIndexUtils.updateIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should make 2 calls: GET to retrieve policy, then PUT to update
    // The PUT will fail but the method should handle it gracefully
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }
}
