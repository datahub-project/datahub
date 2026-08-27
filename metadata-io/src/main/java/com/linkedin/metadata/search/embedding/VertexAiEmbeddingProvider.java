package com.linkedin.metadata.search.embedding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link EmbeddingProvider} that calls Vertex AI's managed embedding endpoint to
 * generate query embeddings.
 *
 * <p>This provider uses Java's built-in HttpClient and supports:
 *
 * <ul>
 *   <li>GCP Vertex AI (aiplatform.googleapis.com)
 *   <li>Token refresh via an injected {@link Supplier} (Workload Identity / GoogleCredentials in
 *       production, stub in tests)
 * </ul>
 *
 * <p>Endpoint pattern: {@code
 * https://<location>-aiplatform.googleapis.com/v1/projects/<project>/locations/<location>/publishers/google/models/<model>:predict}
 *
 * <p>Retry behaviour: up to {@value #MAX_ATTEMPTS} attempts for transient failures. Transient is
 * defined as: network {@link IOException}/{@link InterruptedException}, HTTP 429, or HTTP 5xx. HTTP
 * 4xx (other than 429) is not retried.
 */
@Slf4j
public class VertexAiEmbeddingProvider implements EmbeddingProvider {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  private static final int MAX_ATTEMPTS = 3;

  private static final String URL_TEMPLATE =
      "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict";

  private final String project;
  private final String location;
  private final String defaultModel;
  private final int outputDimensionality;
  private final Supplier<String> tokenSupplier;
  private final HttpClient httpClient;

  /**
   * Creates a new VertexAiEmbeddingProvider.
   *
   * @param project GCP project ID
   * @param location GCP region (e.g. "us-central1")
   * @param defaultModel Default model name (e.g. "gemini-embedding-001")
   * @param outputDimensionality Number of embedding dimensions
   * @param tokenSupplier Supplies Bearer tokens (refreshed on each call)
   */
  public VertexAiEmbeddingProvider(
      @Nonnull final String project,
      @Nonnull final String location,
      @Nonnull final String defaultModel,
      final int outputDimensionality,
      @Nonnull final Supplier<String> tokenSupplier) {
    this(
        project,
        location,
        defaultModel,
        outputDimensionality,
        tokenSupplier,
        HttpClient.newBuilder()
            .connectTimeout(DEFAULT_TIMEOUT)
            .version(HttpClient.Version.HTTP_1_1)
            .build());
  }

  /** Package-private constructor that accepts a custom {@link HttpClient} for testing. */
  VertexAiEmbeddingProvider(
      @Nonnull final String project,
      @Nonnull final String location,
      @Nonnull final String defaultModel,
      final int outputDimensionality,
      @Nonnull final Supplier<String> tokenSupplier,
      @Nonnull final HttpClient httpClient) {
    this.project = Objects.requireNonNull(project, "project cannot be null");
    this.location = Objects.requireNonNull(location, "location cannot be null");
    this.defaultModel = Objects.requireNonNull(defaultModel, "defaultModel cannot be null");
    this.outputDimensionality = outputDimensionality;
    this.tokenSupplier = Objects.requireNonNull(tokenSupplier, "tokenSupplier cannot be null");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient cannot be null");

    log.info(
        "Initialized VertexAiEmbeddingProvider project={}, location={}, model={}, dim={}",
        project,
        location,
        defaultModel,
        outputDimensionality);
  }

  @Override
  @Nonnull
  public float[] embed(@Nonnull final String text, @Nullable final String model) {
    // Default to QUERY because this provider lives behind EmbeddingProvider whose primary
    // use is search-side query embedding. Defaulting to DOCUMENT would silently produce
    // RETRIEVAL_DOCUMENT vectors for query-time callers — opposite of what search expects.
    // Ingestion-side callers should pass DOCUMENT explicitly via the 3-arg overload.
    return embed(text, model, EmbeddingTaskType.QUERY);
  }

  @Override
  @Nonnull
  public float[] embed(
      @Nonnull final String text,
      @Nullable final String model,
      @Nonnull final EmbeddingTaskType taskType) {
    Objects.requireNonNull(text, "text cannot be null");
    Objects.requireNonNull(taskType, "taskType cannot be null");

    String effectiveModel = model != null ? model : defaultModel;
    URI uri = URI.create(String.format(URL_TEMPLATE, location, project, location, effectiveModel));
    String requestBody = buildRequestBody(text, taskType);

    Exception lastException = null;

    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      // Build the request fresh each attempt so retries pick up a refreshed bearer token —
      // GoogleCredentials may rotate the token between attempts.
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(uri)
              .timeout(DEFAULT_TIMEOUT)
              .header("Authorization", "Bearer " + tokenSupplier.get())
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(requestBody))
              .build();

      try {
        HttpResponse<String> response =
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();

        if (status == 200) {
          return parseResponse(response.body());
        }

        if (status == 401 || status == 403) {
          // Auth failures are never transient — fail immediately
          String body = response.body();
          throw new RuntimeException(
              String.format(
                  "Vertex AI auth failure (%d) for project=%s location=%s: %.500s",
                  status, project, location, body != null ? body : "(no body)"));
        }

        if (status == 429 || status >= 500) {
          // Transient: rate-limit or server error — eligible for retry
          String body = response.body();
          String msg =
              String.format(
                  "Vertex AI returned %d for model %s (attempt %d/%d)",
                  status, effectiveModel, attempt, MAX_ATTEMPTS);
          if (attempt >= MAX_ATTEMPTS) {
            // Preserve last response body so debugging context isn't lost on retry exhaustion.
            throw new RuntimeException(
                String.format(
                    "Vertex AI call failed after %d attempts: %s, body=%.500s",
                    MAX_ATTEMPTS, msg, body != null ? body : "(no body)"));
          }
          log.warn("{}, retrying", msg);
          lastException = new RuntimeException(msg);
          backoff(attempt);
          continue;
        }

        // Other 4xx — not retryable
        String body = response.body();
        log.warn("Vertex AI returned {} for model {}: {}", status, effectiveModel, body);
        throw new RuntimeException(
            String.format(
                "Vertex AI returned %d for model %s: %.200s",
                status, effectiveModel, body != null ? body : ""));

      } catch (RuntimeException e) {
        // RuntimeExceptions from the auth-failure or non-retryable 4xx branches propagate
        // immediately
        throw e;
      } catch (InterruptedException e) {
        // Restore the interrupt flag and abort — do not retry after an interruption.
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Vertex AI embedding interrupted for model " + effectiveModel, e);
      } catch (IOException e) {
        lastException = e;
        if (attempt >= MAX_ATTEMPTS) {
          break;
        }
        log.warn(
            "Vertex AI network error on attempt {}/{} for model {}: {}",
            attempt,
            MAX_ATTEMPTS,
            effectiveModel,
            e.getMessage());
        try {
          backoff(attempt);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(
              "Vertex AI embedding interrupted for model " + effectiveModel, ie);
        }
      }
    }

    log.error(
        "All {} attempts failed for Vertex AI embedding with model {}",
        MAX_ATTEMPTS,
        effectiveModel);
    throw new RuntimeException(
        String.format(
            "Vertex AI call failed for model %s after %d attempts: %s",
            effectiveModel,
            MAX_ATTEMPTS,
            lastException != null ? lastException.getMessage() : "unknown error"),
        lastException);
  }

  private String buildRequestBody(String text, EmbeddingTaskType taskType) {
    String vertexTaskType =
        taskType == EmbeddingTaskType.QUERY ? "RETRIEVAL_QUERY" : "RETRIEVAL_DOCUMENT";
    try {
      Map<String, Object> requestMap = new HashMap<>();
      requestMap.put("instances", List.of(Map.of("task_type", vertexTaskType, "content", text)));
      if (outputDimensionality > 0) {
        requestMap.put("parameters", Map.of("outputDimensionality", outputDimensionality));
      }
      return MAPPER.writeValueAsString(requestMap);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize Vertex AI request body", e);
    }
  }

  private static float[] parseResponse(String body) {
    try {
      JsonNode root = MAPPER.readTree(body);
      JsonNode predictions = root.path("predictions");
      if (!predictions.isArray() || predictions.isEmpty()) {
        throw new RuntimeException(
            "Invalid Vertex AI response: predictions array is missing or empty");
      }
      JsonNode values = predictions.get(0).path("embeddings").path("values");
      if (values == null || values.isMissingNode() || !values.isArray()) {
        throw new RuntimeException(
            "Invalid Vertex AI response: missing predictions[0].embeddings.values");
      }
      if (values.size() == 0) {
        throw new RuntimeException(
            "Vertex AI returned empty embedding vector in predictions[0].embeddings.values");
      }
      float[] out = new float[values.size()];
      for (int i = 0; i < values.size(); i++) {
        JsonNode vNode = values.get(i);
        if (!vNode.isNumber()) {
          throw new RuntimeException(
              "Vertex AI returned non-numeric value at predictions[0].embeddings.values["
                  + i
                  + "]: "
                  + vNode);
        }
        out[i] = (float) vNode.asDouble();
      }
      return out;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      log.warn("Failed to parse Vertex AI response: {}", body);
      throw new RuntimeException("Failed to parse Vertex AI response (see log for full body)", e);
    }
  }

  private static void backoff(int attempt) throws InterruptedException {
    // Exponential backoff: 200ms before attempt 2, 400ms before attempt 3.
    Thread.sleep((long) Math.pow(2, attempt) * 100L);
  }
}
