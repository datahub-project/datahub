package com.linkedin.metadata.search.indexbuilder;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IndexExistenceCheckTest {

  @Mock private RestHighLevelClient _searchClient;

  @Mock private IndicesClient indicesClient;

  @Mock private GetIndexRequest getIndexRequest;

  private TestIndexOperationClass testClass;

  // Test class that contains the code under test
  private class TestIndexOperationClass {
    private final RestHighLevelClient _searchClient;

    public TestIndexOperationClass(RestHighLevelClient searchClient) {
      this._searchClient = searchClient;
    }

    public Map<String, Object> checkIndexAndPerformOperation(GetIndexRequest getIndexRequest)
        throws IOException {
      Map<String, Object> result = new HashMap<>();

      // This is the code under test
      if (!_searchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
        result.put("skipped", true);
        result.put("reason", "Index does not exist");
        return result;
      }

      // Mock the operation that would happen if index exists
      result.put("success", true);
      result.put("operation", "completed");
      return result;
    }
  }

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(_searchClient.indices()).thenReturn(indicesClient);
    testClass = new TestIndexOperationClass(_searchClient);
  }

  @Test
  public void testIndexDoesNotExist() throws IOException {
    // Setup
    when(indicesClient.exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT))).thenReturn(false);

    // Execute
    Map<String, Object> result = testClass.checkIndexAndPerformOperation(getIndexRequest);

    // Verify
    assertTrue((Boolean) result.get("skipped"));
    assertEquals("Index does not exist", result.get("reason"));
    assertNull(result.get("success"));
    assertNull(result.get("operation"));

    // Verify the method was called
    verify(indicesClient).exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testIndexExists() throws IOException {
    // Setup
    when(indicesClient.exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT))).thenReturn(true);

    // Execute
    Map<String, Object> result = testClass.checkIndexAndPerformOperation(getIndexRequest);

    // Verify
    assertNull(result.get("skipped"));
    assertNull(result.get("reason"));
    assertTrue((Boolean) result.get("success"));
    assertEquals("completed", result.get("operation"));

    // Verify the method was called
    verify(indicesClient).exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testExceptionDuringExistenceCheck() throws IOException {
    // Setup
    when(indicesClient.exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT)))
        .thenThrow(new IOException("Connection error"));
    try {
      testClass.checkIndexAndPerformOperation(getIndexRequest);
      fail("Expected IOException was not thrown");
    } catch (IOException exception) {
      // Verify the exception message
      assertEquals("Connection error", exception.getMessage());
      // Verify the method was called
      verify(indicesClient).exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT));
    }
  }

  @Test
  public void testNullSearchClient() throws IOException {
    // Setup
    testClass = new TestIndexOperationClass(null);
    try {
      testClass.checkIndexAndPerformOperation(getIndexRequest);
      fail("Expected NullPointerException was not thrown");
    } catch (NullPointerException exception) {
      // No additional assertions needed
    }
  }

  @Test
  public void testIndicesClientReturnsNull() throws IOException {
    // Setup
    when(_searchClient.indices()).thenReturn(null);

    try {
      testClass.checkIndexAndPerformOperation(getIndexRequest);
      fail("Expected NullPointerException was not thrown");
    } catch (NullPointerException exception) {
      // No additional assertions needed
    }
  }

  @Test
  public void testMultipleCallsToExists() throws IOException {
    // Setup
    when(indicesClient.exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT))).thenReturn(false);

    // Execute multiple times
    testClass.checkIndexAndPerformOperation(getIndexRequest);
    testClass.checkIndexAndPerformOperation(getIndexRequest);
    testClass.checkIndexAndPerformOperation(getIndexRequest);

    // Verify the method was called exactly 3 times
    verify(indicesClient, times(3)).exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testWithDifferentRequestOptions() throws IOException {
    // This test verifies that we're using RequestOptions.DEFAULT specifically

    // Setup - mock any request options to return true
    when(indicesClient.exists(any(), any())).thenReturn(true);
    // But specifically mock DEFAULT options with getIndexRequest to return false
    when(indicesClient.exists(eq(getIndexRequest), eq(RequestOptions.DEFAULT))).thenReturn(false);

    // Execute
    Map<String, Object> result = testClass.checkIndexAndPerformOperation(getIndexRequest);

    // Verify we get the "skipped" result because we used DEFAULT options
    assertTrue((Boolean) result.get("skipped"));
    assertEquals("Index does not exist", result.get("reason"));
  }
}
