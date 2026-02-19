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

/** Unit tests for OpenAIEmbeddingProvider. */
public class OpenAIEmbeddingProviderTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private OpenAIEmbeddingProvider provider;

  @BeforeMethod
  public void setup() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    provider =
        new OpenAIEmbeddingProvider(
            "sk-test-key",
            "https://api.openai.com/v1/embeddings",
            "text-embedding-3-small",
            mockHttpClient);
  }

  @Test
  public void testEmbedSuccess() throws Exception {
    // Mock OpenAI response with a simple embedding
    String responseJson =
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.1, 0.2, 0.3], \"index\": 0}], \"model\": \"text-embedding-3-small\", \"usage\": {\"prompt_tokens\": 5, \"total_tokens\": 5}}";

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
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.5, 0.6], \"index\": 0}], \"model\": \"text-embedding-3-large\", \"usage\": {\"prompt_tokens\": 3, \"total_tokens\": 3}}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    // Test with custom model
    float[] embedding = provider.embed("test", "text-embedding-3-large");

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  public void testEmbedWith1536Dimensions() throws Exception {
    // Mock realistic OpenAI text-embedding-3-small response with 1536 dimensions
    StringBuilder embeddingArray = new StringBuilder("[");
    for (int i = 0; i < 1536; i++) {
      if (i > 0) embeddingArray.append(", ");
      embeddingArray.append(String.format("%.6f", Math.random()));
    }
    embeddingArray.append("]");

    String responseJson =
        String.format(
            "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": %s, \"index\": 0}], \"model\": \"text-embedding-3-small\", \"usage\": {\"prompt_tokens\": 5, \"total_tokens\": 5}}",
            embeddingArray);

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(
        embedding.length, 1536, "OpenAI text-embedding-3-small should return 1536 dimensions");
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithHttpError401() throws Exception {
    // Mock 401 Unauthorized response — not retried
    String errorJson =
        "{\"error\": {\"message\": \"Invalid API key\", \"type\": \"invalid_request_error\"}}";

    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn(errorJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithHttpError429() throws Exception {
    // Mock 429 Rate Limit response — not retried
    String errorJson =
        "{\"error\": {\"message\": \"Rate limit exceeded\", \"type\": \"rate_limit_error\"}}";

    when(mockResponse.statusCode()).thenReturn(429);
    when(mockResponse.body()).thenReturn(errorJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithHttpError500() throws Exception {
    // Mock 500 Internal Server Error — retried
    String errorJson =
        "{\"error\": {\"message\": \"Internal server error\", \"type\": \"server_error\"}}";

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
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.1, 0.2, 0.3], \"index\": 0}], \"model\": \"text-embedding-3-small\", \"usage\": {\"prompt_tokens\": 5, \"total_tokens\": 5}}";

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
  public void testEmbedWithMissingData() throws Exception {
    // Mock response without data field
    String responseJson = "{\"object\": \"list\", \"model\": \"text-embedding-3-small\"}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithEmptyDataArray() throws Exception {
    // Mock response with empty data array
    String responseJson =
        "{\"object\": \"list\", \"data\": [], \"model\": \"text-embedding-3-small\"}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testEmbedWithMissingEmbedding() throws Exception {
    // Mock response without embedding field
    String responseJson =
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"index\": 0}], \"model\": \"text-embedding-3-small\"}";

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
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [\"invalid\", \"values\"], \"index\": 0}], \"model\": \"text-embedding-3-small\"}";

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
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.1], \"index\": 0}], \"model\": \"text-embedding-3-small\", \"usage\": {\"prompt_tokens\": 0, \"total_tokens\": 0}}";

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
    OpenAIEmbeddingProvider simpleProvider = new OpenAIEmbeddingProvider("sk-test-key");
    assertNotNull(simpleProvider);
  }

  @Test
  public void testConstructorWithAllParameters() {
    // Test constructor with all parameters
    OpenAIEmbeddingProvider fullProvider =
        new OpenAIEmbeddingProvider(
            "sk-test-key",
            "https://custom-endpoint.example.com/v1/embeddings",
            "text-embedding-3-large");
    assertNotNull(fullProvider);
  }

  @Test
  public void testMultipleEmbedCalls() throws Exception {
    // Test multiple embed calls work correctly
    String responseJson1 =
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.1, 0.2], \"index\": 0}], \"model\": \"text-embedding-3-small\", \"usage\": {\"prompt_tokens\": 5, \"total_tokens\": 5}}";
    String responseJson2 =
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.3, 0.4], \"index\": 0}], \"model\": \"text-embedding-3-small\", \"usage\": {\"prompt_tokens\": 5, \"total_tokens\": 5}}";

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
