package com.linkedin.metadata.search.embedding;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for VertexAiEmbeddingProvider. */
public class VertexAiEmbeddingProviderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TEST_PROJECT = "my-project";
  private static final String TEST_LOCATION = "us-central1";
  private static final String TEST_MODEL = "gemini-embedding-001";
  private static final int TEST_DIM = 3072;
  private static final String TEST_TOKEN = "test-bearer-token";

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private VertexAiEmbeddingProvider provider;

  @BeforeMethod
  public void setup() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    provider =
        new VertexAiEmbeddingProvider(
            TEST_PROJECT, TEST_LOCATION, TEST_MODEL, TEST_DIM, () -> TEST_TOKEN, mockHttpClient);
  }

  @Test
  public void parsesEmbeddingFromVertexResponse() throws Exception {
    String responseJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.1, 0.2, 0.3]}}]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] embedding = provider.embed("hello world", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 3);
    assertEquals(embedding[0], 0.1f, 0.001f);
    assertEquals(embedding[1], 0.2f, 0.001f);
    assertEquals(embedding[2], 0.3f, 0.001f);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  public void sendsBearerTokenAuthHeader() throws Exception {
    String responseJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.5, 0.6]}}]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    when(mockHttpClient.send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test text", null);

    HttpRequest captured = requestCaptor.getValue();
    assertTrue(
        captured.headers().firstValue("Authorization").isPresent(),
        "Authorization header must be present");
    assertEquals(
        captured.headers().firstValue("Authorization").get(),
        "Bearer " + TEST_TOKEN,
        "Authorization header must be 'Bearer <token>'");
  }

  @Test
  public void usesModelOverrideWhenProvided() throws Exception {
    String responseJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.7, 0.8]}}]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    when(mockHttpClient.send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] embedding = provider.embed("test", "text-embedding-005");

    assertNotNull(embedding);
    assertEquals(embedding.length, 2);

    // URL should contain the overridden model name
    String requestUri = requestCaptor.getValue().uri().toString();
    assertTrue(
        requestUri.contains("text-embedding-005"), "URL should contain the overridden model");
    assertFalse(requestUri.contains(TEST_MODEL), "URL should not contain the default model");
  }

  @Test
  public void buildUrlWithCorrectProjectAndLocation() throws Exception {
    String responseJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.1]}}]}";

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    when(mockHttpClient.send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);

    String uri = requestCaptor.getValue().uri().toString();
    assertTrue(uri.contains(TEST_PROJECT), "URL must contain the project: " + uri);
    assertTrue(uri.contains(TEST_LOCATION), "URL must contain the location: " + uri);
    assertTrue(uri.contains(TEST_MODEL), "URL must contain the model: " + uri);
    assertTrue(uri.contains("aiplatform.googleapis.com"), "URL must target Vertex AI: " + uri);
  }

  @Test
  public void publicConstructorWorks() {
    // Verifies the public constructor (used in production) doesn't throw
    VertexAiEmbeddingProvider publicProvider =
        new VertexAiEmbeddingProvider(
            TEST_PROJECT, TEST_LOCATION, TEST_MODEL, TEST_DIM, () -> TEST_TOKEN);
    assertNotNull(publicProvider);
  }

  /** Reads the body of a Java 11 HttpRequest by subscribing to its BodyPublisher. */
  private static String readRequestBody(HttpRequest request) throws Exception {
    LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    request
        .bodyPublisher()
        .ifPresent(
            publisher ->
                publisher.subscribe(
                    new Flow.Subscriber<ByteBuffer>() {
                      @Override
                      public void onSubscribe(Flow.Subscription subscription) {
                        subscription.request(Long.MAX_VALUE);
                      }

                      @Override
                      public void onNext(ByteBuffer item) {
                        byte[] bytes = new byte[item.remaining()];
                        item.get(bytes);
                        queue.add(bytes);
                      }

                      @Override
                      public void onError(Throwable throwable) {}

                      @Override
                      public void onComplete() {
                        queue.add(new byte[0]); // sentinel
                      }
                    }));

    StringBuilder sb = new StringBuilder();
    byte[] chunk;
    while ((chunk = queue.poll(5, TimeUnit.SECONDS)) != null && chunk.length > 0) {
      sb.append(new String(chunk, StandardCharsets.UTF_8));
    }
    return sb.toString();
  }

  @Test
  public void embedWithQueryTaskTypeSucceeds() throws Exception {
    // Verifies the three-arg embed overload works end-to-end with QUERY task type
    String responseJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.1, 0.2, 0.3]}}]}";
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    when(mockHttpClient.send(captor.capture(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] result = provider.embed("find tables with PII", null, EmbeddingTaskType.QUERY);

    assertNotNull(result);
    assertEquals(result.length, 3);
    assertEquals(result[0], 0.1f, 0.001f);

    // Parse JSON body to assert structured field — prevents false-pass from substring matches
    JsonNode root = MAPPER.readTree(readRequestBody(captor.getValue()));
    assertEquals(
        root.path("instances").get(0).path("task_type").asText(),
        "RETRIEVAL_QUERY",
        "instances[0].task_type must be RETRIEVAL_QUERY for QUERY task");
  }

  @Test
  public void embedWithDocumentTaskTypeSucceeds() throws Exception {
    String responseJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.5]}}]}";
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    when(mockHttpClient.send(captor.capture(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] result = provider.embed("a document to index", null, EmbeddingTaskType.DOCUMENT);

    assertNotNull(result);
    assertEquals(result.length, 1);
    assertEquals(result[0], 0.5f, 0.001f);

    JsonNode root = MAPPER.readTree(readRequestBody(captor.getValue()));
    assertEquals(
        root.path("instances").get(0).path("task_type").asText(),
        "RETRIEVAL_DOCUMENT",
        "instances[0].task_type must be RETRIEVAL_DOCUMENT for DOCUMENT task");
  }

  @Test
  public void omitsOutputDimensionalityWhenZero() throws Exception {
    // Provider with outputDimensionality=0 should not include "parameters" in the request body
    VertexAiEmbeddingProvider zeroDimProvider =
        new VertexAiEmbeddingProvider(
            TEST_PROJECT, TEST_LOCATION, TEST_MODEL, 0, () -> TEST_TOKEN, mockHttpClient);

    String responseJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.1, 0.2]}}]}";
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    when(mockHttpClient.send(captor.capture(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] result = zeroDimProvider.embed("test", null);
    assertNotNull(result);
    assertEquals(result.length, 2);

    JsonNode root = MAPPER.readTree(readRequestBody(captor.getValue()));
    assertTrue(
        root.path("parameters").isMissingNode(),
        "parameters should be absent when outputDimensionality=0; got: " + root);
  }

  @Test
  public void defaultEmbedUsesQueryTaskType() throws Exception {
    // embed(text, model) without task type should default to QUERY because this provider
    // lives behind EmbeddingProvider whose primary use is search-side query embedding.
    String responseJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.1]}}]}";
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    when(mockHttpClient.send(captor.capture(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    float[] result = provider.embed("some document", null);
    assertNotNull(result);
    assertEquals(result.length, 1);

    JsonNode root = MAPPER.readTree(readRequestBody(captor.getValue()));
    assertEquals(
        root.path("instances").get(0).path("task_type").asText(),
        "RETRIEVAL_QUERY",
        "Default task type must be RETRIEVAL_QUERY; got: " + root);
  }

  // -------------------------------------------------------------------------
  // Error path tests
  // -------------------------------------------------------------------------

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*auth failure.*")
  public void bubbles401WithoutRetry() throws Exception {
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("{\"error\": \"Unauthorized\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    try {
      provider.embed("test", null);
    } finally {
      // 401 must not be retried — exactly 1 call
      verify(mockHttpClient, times(1))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*auth failure.*")
  public void bubbles403WithoutRetry() throws Exception {
    when(mockResponse.statusCode()).thenReturn(403);
    when(mockResponse.body()).thenReturn("{\"error\": \"Forbidden\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    try {
      provider.embed("test", null);
    } finally {
      verify(mockHttpClient, times(1))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test
  public void retriesOn429AndSucceeds() throws Exception {
    HttpResponse<String> rateLimitResponse = mock(HttpResponse.class);
    when(rateLimitResponse.statusCode()).thenReturn(429);
    when(rateLimitResponse.body()).thenReturn("{\"error\": \"rate limit\"}");

    String successJson = "{\"predictions\": [{\"embeddings\": {\"values\": [0.1, 0.2, 0.3]}}]}";
    HttpResponse<String> successResponse = mock(HttpResponse.class);
    when(successResponse.statusCode()).thenReturn(200);
    when(successResponse.body()).thenReturn(successJson);

    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(rateLimitResponse)
        .thenReturn(successResponse);

    float[] embedding = provider.embed("test", null);

    assertNotNull(embedding);
    assertEquals(embedding.length, 3);
    assertEquals(embedding[0], 0.1f, 0.001f);

    // Should have been called exactly twice: once for the 429, once for the 200
    verify(mockHttpClient, times(2))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void bubbles500AfterMaxAttempts() throws Exception {
    when(mockResponse.statusCode()).thenReturn(500);
    when(mockResponse.body()).thenReturn("{\"error\": \"Internal Server Error\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    try {
      provider.embed("test", null);
    } finally {
      // Must have retried MAX_ATTEMPTS (3) times before giving up
      verify(mockHttpClient, times(3))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void retriesOnIOExceptionAndExhausts() throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Connection reset"));

    try {
      provider.embed("test", null);
    } finally {
      verify(mockHttpClient, times(3))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void throwsOnMalformedResponse() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("not valid json{{{{");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Invalid Vertex AI response.*")
  public void throwsOnResponseMissingEmbeddingsValues() throws Exception {
    // Valid JSON but missing the expected predictions[0].embeddings.values array
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"predictions\": [{\"embeddings\": {}}]}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*empty embedding vector.*")
  public void throwsOnEmptyValuesArray() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"predictions\": [{\"embeddings\": {\"values\": []}}]}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*predictions array is missing or empty.*")
  public void throwsOnEmptyPredictionsArray() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"predictions\": []}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*predictions array is missing or empty.*")
  public void throwsOnMissingPredictionsKey() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp =
          ".*non-numeric value at predictions\\[0\\].embeddings.values\\[1\\].*")
  public void throwsOnNonNumericValueInEmbeddingVector() throws Exception {
    // Vertex AI should never return non-numeric values, but a guard is required to prevent
    // silent vector corruption (asDouble() returns 0.0 for non-numeric nodes).
    String responseJson =
        "{\"predictions\": [{\"embeddings\": {\"values\": [0.1, \"bad\", 0.3]}}]}";
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseJson);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.embed("test", null);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Vertex AI call failed after.*")
  public void exhausts429RetriesAndThrows() throws Exception {
    when(mockResponse.statusCode()).thenReturn(429);
    when(mockResponse.body()).thenReturn("{\"error\": \"quota exceeded\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    try {
      provider.embed("test", null);
    } finally {
      // All 3 attempts should have been made before giving up on 429
      verify(mockHttpClient, times(3))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Vertex AI returned 404.*")
  public void throwsNonRetryable4xxImmediately() throws Exception {
    when(mockResponse.statusCode()).thenReturn(404);
    when(mockResponse.body()).thenReturn("{\"error\": \"model not found\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    try {
      provider.embed("test", null);
    } finally {
      // Non-retryable 4xx must not be retried
      verify(mockHttpClient, times(1))
          .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
  }

  @Test
  public void interruptedExceptionRestoresFlagAndThrows() throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new InterruptedException("test interrupt"));

    RuntimeException thrown = null;
    try {
      provider.embed("test", null);
    } catch (RuntimeException e) {
      thrown = e;
    } finally {
      // Verify the interrupt flag was restored; clear it so it doesn't bleed into other tests
      boolean interruptFlagRestored = Thread.interrupted();
      assertTrue(
          interruptFlagRestored,
          "Thread interrupt flag must be restored after InterruptedException");
    }
    assertNotNull(thrown, "RuntimeException must be thrown on InterruptedException");
    assertTrue(
        thrown.getMessage().toLowerCase().contains("interrupted"),
        "Exception message should mention 'interrupted'; got: " + thrown.getMessage());
  }
}
