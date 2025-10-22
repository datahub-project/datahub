package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UsageEventIndexUtilsTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  @Mock private SearchClientShim searchClient;
  @Mock private RawResponse rawResponse;
  @Mock private CreateIndexResponse createIndexResponse;
  @Mock private ResponseException responseException;
  @Mock private OpenSearchStatusException openSearchStatusException;

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

  @Test(expectedExceptions = ResponseException.class)
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
    UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
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
}
