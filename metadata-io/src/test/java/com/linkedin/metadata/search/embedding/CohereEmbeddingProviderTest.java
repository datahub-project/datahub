package com.linkedin.metadata.search.embedding;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for CohereEmbeddingProvider. */
public class CohereEmbeddingProviderTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private CohereEmbeddingProvider provider;

  @BeforeMethod
  public void setup() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    provider =
        new CohereEmbeddingProvider(
            "test-api-key", "https://api.cohere.ai/v1/embed", "embed-english-v3.0", mockHttpClient);
  }

  @Test
  public void testEmbedSuccess() throws Exception {
    // Mock Cohere response with a simple embedding
    String responseJson =
        "{\"embeddings\": [[0.1, 0.2, 0.3]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"test query\"]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    // Test embedding generation
    float[] embedding = provider.embed("test query", null);

    // Verify results
    assertNotNull(embedding);
    assertEquals(embedding.length, 3);
    assertEquals(embedding[0], 0.1f, 0.001);
    assertEquals(embedding[1], 0.2f, 0.001);
    assertEquals(embedding[2], 0.3f, 0.001);

    // Verify the request was made
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  public void testEmbedWithCustomModel() throws Exception {
    String responseJson =
        "{\"embeddings\": [[0.5, 0.6]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"test\"]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    // Test with custom model
    float[] embedding = provider.embed("test", "embed-multilingual-v3.0");

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  public void testEmbedWith1024Dimensions() throws Exception {
    // Mock realistic Cohere embed-english-v3.0 response with 1024 dimensions
    StringBuilder embeddingArray = new StringBuilder("[");
    for (int i = 0; i < 1024; i++) {
      if (i > 0) embeddingArray.append(", ");
      embeddingArray.append(String.format("%.6f", Math.random()));
    }
    embeddingArray.append("]");

    String responseJson =
        String.format(
            "{\"embeddings\": [%s], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"test query\"]}",
            embeddingArray);

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 1024, "Cohere embed-english-v3.0 should return 1024 dimensions");
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithHttpError401() throws Exception {
    // Mock 401 Unauthorized response — not retried
    String errorJson = "{\"message\": \"Invalid API key\"}";

    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn(errorJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithHttpError429() throws Exception {
    // Mock 429 Rate Limit response — not retried
    String errorJson = "{\"message\": \"Rate limit exceeded\"}";

    when(mockResponse.statusCode()).thenReturn(429);
    when(mockResponse.body()).thenReturn(errorJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithHttpError500() throws Exception {
    // Mock 500 Internal Server Error — retried
    String errorJson = "{\"message\": \"Internal server error\"}";

    when(mockResponse.statusCode()).thenReturn(500);
    when(mockResponse.body()).thenReturn(errorJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    try {
      provider.embed("test", null);
    } finally {
      verify(mockHttpClient, times(2))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test
  public void testRetrySucceedsOnSecondAttempt() throws Exception {
    String successJson =
        "{\"embeddings\": [[0.1, 0.2, 0.3]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"test query\"]}";

    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Connection reset"))
        .thenReturn(mockResponse);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(successJson);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 3);
    assertEquals(embedding[0], 0.1f, 0.001);
    verify(mockHttpClient, times(2))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testRetryExhaustedThrowsAfterAllAttempts() throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Connection reset"));

    try {
      provider.embed("test query", null);
    } finally {
      verify(mockHttpClient, times(2))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithMissingEmbeddings() throws Exception {
    // Mock response without embeddings field
    String responseJson = "{\"id\": \"test-id\", \"response_type\": \"embeddings_floats\"}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithEmptyEmbeddingsArray() throws Exception {
    // Mock response with empty embeddings array
    String responseJson =
        "{\"embeddings\": [], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\"}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithInvalidJson() throws Exception {
    // Mock response with invalid JSON
    String responseJson = "invalid json{{{";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithNonNumericValues() throws Exception {
    // Mock response with non-numeric embedding values
    String responseJson =
        "{\"embeddings\": [[\"invalid\", \"values\"]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\"}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithIOException() throws Exception {
    // Mock IOException
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Network error"));

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithInterruptedException() throws Exception {
    // Mock InterruptedException
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new InterruptedException("Request interrupted"));

    provider.embed("test", null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testEmbedWithNullText() {
    provider.embed(null, null);
  }

  @Test
  public void testEmbedWithEmptyText() throws Exception {
    String responseJson =
        "{\"embeddings\": [[0.1]], \"id\": \"test-id\", \"response_type\": \"embeddings_floats\", \"texts\": [\"\"]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    // Empty text should still work
    float[] embedding = provider.embed("", null);

    assertNotNull(embedding);
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  public void testConstructorWithApiKey() {
    // Test that simple constructor doesn't throw
    CohereEmbeddingProvider simpleProvider = new CohereEmbeddingProvider("test-api-key");
    assertNotNull(simpleProvider);
  }

  @Test
  public void testConstructorWithAllParameters() {
    // Test constructor with all parameters
    CohereEmbeddingProvider fullProvider =
        new CohereEmbeddingProvider(
            "test-api-key",
            "https://custom-endpoint.example.com/v1/embed",
            "embed-multilingual-v3.0");
    assertNotNull(fullProvider);
  }

  @Test
  public void testMultipleEmbedCalls() throws Exception {
    // Test multiple embed calls work correctly
    String responseJson1 =
        "{\"embeddings\": [[0.1, 0.2]], \"id\": \"test-id-1\", \"response_type\": \"embeddings_floats\", \"texts\": [\"first query\"]}";
    String responseJson2 =
        "{\"embeddings\": [[0.3, 0.4]], \"id\": \"test-id-2\", \"response_type\": \"embeddings_floats\", \"texts\": [\"second query\"]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson1).thenReturn(responseJson2);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] embedding1 = provider.embed("first query", null);
    float[] embedding2 = provider.embed("second query", null);

    assertNotNull(embedding1);
    assertNotNull(embedding2);
    assertEquals(embedding1[0], 0.1f, 0.001);
    assertEquals(embedding2[0], 0.3f, 0.001);
    verify(mockHttpClient, times(2))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }
}
