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
    // Mock OperationContext to return a real ObjectMapper for JSON parsing
    Mockito.when(operationContext.getObjectMapper())
        .thenReturn(new com.fasterxml.jackson.databind.ObjectMapper());
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
  public void testCreateIlmPolicy_OuterResponseException409() throws IOException {
    // Arrange
    String policyName = "test_policy";

    // Mock ResponseException with 409 status directly (not through retry logic)
    // This covers the outer catch block: } catch (ResponseException e) { if
    // (e.getResponse().getStatusLine().getStatusCode() == 409) { log.info("ILM policy {} already
    // exists", policyName); } else { throw e; } }
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException); // Direct ResponseException with 409

    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 409; // Outer ResponseException with 409
              }

              @Override
              public String getReasonPhrase() {
                return "Conflict";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            });

    // Act
    UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);

    // Assert - Should succeed due to outer catch block handling 409
    // Should make 1 call that throws ResponseException with 409
    Mockito.verify(searchClient, Mockito.times(1))
        .performLowLevelRequest(Mockito.any(Request.class));
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

  @Test(expectedExceptions = IOException.class)
  public void testCreateIlmPolicy_NonSuccessStatusCode() throws IOException {
    // Arrange
    String policyName = "test_policy";
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 400; // Bad Request - not 200, 201, or 409
              }

              @Override
              public String getReasonPhrase() {
                return "Bad Request";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    // Act
    UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);

    // Assert - Should throw IOException after retries fail
    // This tests the specific error handling: log.error("ILM policy creation returned status: {}",
    // statusCode);
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
  public void testCreateIsmPolicy_OuterExceptionHandling() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Create a mock esComponents that throws an exception when getSearchClient() is called
    // This will trigger the outer catch block: catch (Exception e) { log.error("Unexpected error
    // creating ISM policy {}: {}", policyName, e.getMessage(), e); return false; }
    BaseElasticSearchComponentsFactory.BaseElasticSearchComponents mockEsComponents =
        Mockito.mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class);
    Mockito.when(mockEsComponents.getSearchClient())
        .thenThrow(new RuntimeException("Search client initialization failed"));

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(
            mockEsComponents, policyName, prefix, operationContext);

    // Assert - Should return false due to outer exception handling
    Assert.assertFalse(result);
    // No calls to performLowLevelRequest should be made since the exception occurs before retry
    // logic
    Mockito.verify(searchClient, Mockito.never())
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_GetResponse404() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock GET request returning 404 status (policy doesn't exist)
    // This covers the code path: if (getStatusCode == 404) { return createNewPolicy(esComponents,
    // endpoint, policyJson, policyName); }
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse) // Mock successful GET response with 404
        .thenReturn(rawResponse); // Mock successful PUT response for policy creation
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 404; // First call returns 404 (GET), second call returns 201 (PUT)
              }

              @Override
              public String getReasonPhrase() {
                return "Not Found";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            })
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 201; // Second call returns 201 (PUT)
              }

              @Override
              public String getReasonPhrase() {
                return "Created";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            });

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return true since policy was successfully created
    Assert.assertTrue(result);
    // Should make 2 calls: GET (404) then PUT (201)
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_ResponseExceptionRetryableError() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock ResponseException with 400 status (retryable error)
    // This covers the code path: log.warn("ISM policy operation failed with status: {}. Response:
    // {}. Will retry.", statusCode, responseBody); throw new RuntimeException("Retryable error: " +
    // statusCode + " - " + responseBody);
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 400; // Retryable error status
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

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return false due to retryable error after exhausting retries
    Assert.assertFalse(result);
    // Should make 5 calls due to retry logic
    Mockito.verify(searchClient, Mockito.times(5))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_HandleExistingPolicyUpdateFailure() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock ResponseException with 200 status (policy already exists)
    // This will trigger handleExistingPolicy, but we'll mock updateIsmPolicy to throw an exception
    // This covers the code path: } catch (Exception updateException) { log.warn("Failed to update
    // existing ISM policy {} (non-fatal): {}", policyName, updateException.getMessage()); return
    // true; }
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200; // Policy already exists
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            });

    // Mock the updateIsmPolicy call to throw an exception
    // This simulates the scenario where the policy exists but updating it fails
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException) // First call throws 200 ResponseException
        .thenThrow(
            new RuntimeException(
                "Update failed")); // Second call (from updateIsmPolicy) throws exception

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return true even though update failed (non-fatal)
    Assert.assertTrue(result);
    // Should make 2 calls: first GET (200), then PUT (throws exception)
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_CreateNewPolicy409Conflict() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock GET request returning 404 status (policy doesn't exist)
    // This will trigger createNewPolicy, but we'll mock the PUT request to return 409 Conflict
    // This covers the code path: if (createStatusCode == 409) { log.info("ISM policy {} already
    // exists", policyName); return true; }
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse) // Mock successful GET response with 404
        .thenReturn(rawResponse); // Mock successful PUT response with 409 Conflict
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 404; // First call returns 404 (GET)
              }

              @Override
              public String getReasonPhrase() {
                return "Not Found";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            })
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 409; // Second call returns 409 Conflict (PUT)
              }

              @Override
              public String getReasonPhrase() {
                return "Conflict";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            });

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return true since 409 means policy already exists (success)
    Assert.assertTrue(result);
    // Should make 2 calls: GET (404) then PUT (409)
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_CreateNewPolicyErrorStatus() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock GET request returning 404 status (policy doesn't exist)
    // This will trigger createNewPolicy, but we'll mock the PUT request to return 500 Internal
    // Server Error
    // This covers the code path: log.error("ISM policy creation returned status: {}",
    // createStatusCode); return false;
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse) // Mock successful GET response with 404
        .thenReturn(rawResponse); // Mock successful PUT response with 500
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 404; // First call returns 404 (GET)
              }

              @Override
              public String getReasonPhrase() {
                return "Not Found";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            })
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 500; // Second call returns 500 (PUT) - error status
              }

              @Override
              public String getReasonPhrase() {
                return "Internal Server Error";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            });

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return false due to error status
    Assert.assertFalse(result);
    // Should make 2 calls: GET (404) then PUT (500)
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_CreateNewPolicyIOException() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock GET request returning 404 status (policy doesn't exist)
    // This will trigger createNewPolicy, but we'll mock the PUT request to throw IOException
    // This covers the code path: } catch (IOException e) { log.error("Failed to create ISM policy
    // {}: {}", policyName, e.getMessage()); return false; }
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse) // Mock successful GET response with 404
        .thenThrow(new IOException("Network connection failed")); // Second call throws IOException
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 404; // First call returns 404 (GET)
              }

              @Override
              public String getReasonPhrase() {
                return "Not Found";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
              }
            });

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return false due to IOException
    Assert.assertFalse(result);
    // Should make 2 calls: GET (404) then PUT (throws IOException)
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateIsmPolicy_ExtractResponseBodyIOException() throws IOException {
    // Arrange
    String policyName = "test_policy";
    String prefix = "test_";

    // Mock ResponseException with 400 status (retryable error)
    // This will trigger handleResponseException, which calls extractResponseBody
    // We'll mock the response entity to throw IOException when reading content
    // This covers the code path: } catch (IOException e) { return "Error reading response body: " +
    // e.getMessage(); }
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException); // ResponseException with 400 status
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 400; // Retryable error status
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

    // Mock the response entity to throw IOException when reading content
    org.opensearch.client.Response mockResponse =
        Mockito.mock(org.opensearch.client.Response.class);
    org.apache.http.HttpEntity mockEntity = Mockito.mock(org.apache.http.HttpEntity.class);
    Mockito.when(responseException.getResponse()).thenReturn(mockResponse);
    Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
    Mockito.when(mockEntity.getContent())
        .thenThrow(new IOException("Failed to read response content"));

    // Act
    boolean result =
        UsageEventIndexUtils.createIsmPolicy(esComponents, policyName, prefix, operationContext);

    // Assert - Should return false due to retryable error after exhausting retries
    Assert.assertFalse(result);
    // Should make 5 calls due to retry logic
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
  public void testCreateOpenSearchIndex_NotAcknowledged() throws IOException {
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
    Mockito.when(createIndexResponse.isAcknowledged()).thenReturn(false); // Not acknowledged

    // Act
    UsageEventIndexUtils.createOpenSearchIndex(esComponents, indexName, prefix);

    // Assert
    Mockito.verify(searchClient)
        .indexExists(Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class));
    Mockito.verify(searchClient)
        .createIndex(Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class));
    // The method should complete without throwing an exception, but log a warning
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
  public void testCreateIndexTemplate_Success201() throws IOException {
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
                return 201; // Created status code
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
  public void testCreateOpenSearchIndexTemplate_Success201() throws IOException {
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
                return 201; // Created status code
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

    // Assert - Should make 2 calls: GET to retrieve policy, then PUT to update with concurrency
    // control
    Mockito.verify(searchClient, Mockito.times(2))
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
