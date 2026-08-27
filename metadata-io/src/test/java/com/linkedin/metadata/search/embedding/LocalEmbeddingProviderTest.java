package com.linkedin.metadata.search.embedding;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for LocalEmbeddingProvider. */
public class LocalEmbeddingProviderTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private LocalEmbeddingProvider provider;

  @BeforeMethod
  public void setup() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    provider =
        new LocalEmbeddingProvider(
            "http://localhost:11434/v1/embeddings", "nomic-embed-text", mockHttpClient);
  }

  @Test
  public void testEmbedSuccess() throws Exception {
    String responseJson =
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.1, 0.2, 0.3], \"index\": 0}], \"model\": \"nomic-embed-text\"}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 3);
    assertEquals(embedding[0], 0.1f, 0.001);
    assertEquals(embedding[1], 0.2f, 0.001);
    assertEquals(embedding[2], 0.3f, 0.001);
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  public void testEmbedWith768Dimensions() throws Exception {
    StringBuilder embeddingArray = new StringBuilder("[");
    for (int i = 0; i < 768; i++) {
      if (i > 0) embeddingArray.append(", ");
      embeddingArray.append(String.format("%.6f", Math.random()));
    }
    embeddingArray.append("]");

    String responseJson =
        String.format(
            "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": %s, \"index\": 0}], \"model\": \"nomic-embed-text\"}",
            embeddingArray);

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 768, "nomic-embed-text should return 768 dimensions");
  }

  @Test
  public void testEmbedWithCustomModel() throws Exception {
    String responseJson =
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.5, 0.6], \"index\": 0}], \"model\": \"mxbai-embed-large\"}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] embedding = provider.embed("test", "mxbai-embed-large");

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*ollama serve.*")
  public void testConnectExceptionSurfacesHelpfulMessage() throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new ConnectException("Connection refused"));

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*ollama pull.*")
  public void testHttp404SurfacesModelPullHint() throws Exception {
    when(mockResponse.statusCode()).thenReturn(404);
    when(mockResponse.body()).thenReturn("{\"error\": \"model not found\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*ollama serve.*")
  public void testWrappedConnectExceptionSurfacesHelpfulMessage() throws Exception {
    // HttpClient sometimes wraps ConnectException inside IOException — ensure the hint still fires.
    IOException wrapped = new IOException("Connection refused");
    wrapped.initCause(new ConnectException("Connection refused"));
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(wrapped);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testIoExceptionRetryExhaustedThrows() throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Connection reset"))
        .thenThrow(new IOException("Connection reset"));

    try {
      provider.embed("test", null);
    } finally {
      verify(mockHttpClient, times(2))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testHttp500IsRetriedThenFails() throws Exception {
    when(mockResponse.statusCode()).thenReturn(500);
    when(mockResponse.body()).thenReturn("{\"error\": \"internal server error\"}");
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
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.1, 0.2], \"index\": 0}], \"model\": \"nomic-embed-text\"}";

    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Connection reset"))
        .thenReturn(mockResponse);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(successJson);

    float[] embedding = provider.embed("test query", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);
    verify(mockHttpClient, times(2))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testInvalidJsonThrows() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("not json{{");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMissingDataArrayThrows() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"object\": \"list\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testNonNumericEmbeddingValuesThrows() throws Exception {
    String responseJson =
        "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [\"bad\", \"values\"], \"index\": 0}]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }
}
