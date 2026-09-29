package com.linkedin.metadata.search.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link EmbeddingProvider} that calls a locally-running OpenAI-compatible
 * embeddings server to generate query embeddings without requiring a cloud API key.
 *
 * <p>Designed for use with Ollama (the primary supported backend), but works with any server that
 * implements the OpenAI {@code /v1/embeddings} API format, including LM Studio and llama.cpp.
 *
 * <p>Quick start with Ollama:
 *
 * <pre>
 *   brew install ollama
 *   ollama pull nomic-embed-text   # 768 dimensions
 *   ollama serve                   # starts on http://localhost:11434
 * </pre>
 *
 * <p>Other popular models:
 *
 * <ul>
 *   <li>{@code mxbai-embed-large} — 1024 dimensions, higher quality
 *   <li>{@code all-minilm} — 384 dimensions, fastest
 *   <li>{@code nomic-embed-text} (default) — 768 dimensions, good quality/speed balance
 * </ul>
 *
 * @see <a href="https://ollama.com/library">Ollama model library</a>
 * @see <a href="https://github.com/ollama/ollama/blob/main/docs/openai.md">Ollama OpenAI
 *     compatibility</a>
 */
@Slf4j
public class LocalEmbeddingProvider implements EmbeddingProvider {

  public static final String DEFAULT_MODEL = "nomic-embed-text";
  public static final String DEFAULT_ENDPOINT = "http://localhost:11434/v1/embeddings";

  // Short connect timeout: if we can't reach the server in 10s it isn't running.
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
  // Long request timeout: cold model loading (GGUF from disk) can take 60s+ on slow hardware.
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(120);
  private static final int MAX_ATTEMPTS = 2;

  private final String endpoint;
  @Nonnull private final String defaultModel;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  public LocalEmbeddingProvider() {
    this(DEFAULT_ENDPOINT, DEFAULT_MODEL);
  }

  public LocalEmbeddingProvider(@Nonnull String endpoint, @Nonnull String defaultModel) {
    this(
        endpoint,
        defaultModel,
        HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .version(HttpClient.Version.HTTP_1_1)
            .build());
  }

  public LocalEmbeddingProvider(
      @Nonnull String endpoint, @Nonnull String defaultModel, @Nonnull HttpClient httpClient) {
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
    this.defaultModel = Objects.requireNonNull(defaultModel, "defaultModel cannot be null");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient cannot be null");
    this.objectMapper = new ObjectMapper();

    log.info(
        "Initialized LocalEmbeddingProvider with endpoint={}, model={}", endpoint, defaultModel);
  }

  @Override
  @Nonnull
  public float[] embed(@Nonnull String text, @Nullable String model) {
    Objects.requireNonNull(text, "text cannot be null");

    @Nonnull String modelToUse = model != null ? model : defaultModel;
    Exception lastException = null;

    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        return embedInternal(text, modelToUse);
      } catch (ConnectException e) {
        // Connection refused — server isn't running. Not retryable.
        throw newConnectError(e);
      } catch (RuntimeException e) {
        lastException = e;
        break;
      } catch (Exception e) {
        // HttpClient sometimes wraps ConnectException in IOException — check cause.
        if (e.getCause() instanceof ConnectException) {
          throw newConnectError(e.getCause());
        }
        lastException = e;
        if (attempt < MAX_ATTEMPTS) {
          log.warn(
              "Local embedding attempt {}/{} failed for model {}, retrying: {}",
              attempt,
              MAX_ATTEMPTS,
              modelToUse,
              e.getMessage());
        }
      }
    }

    log.error("All {} attempts failed for local embedding with model {}", MAX_ATTEMPTS, modelToUse);
    Exception cause = Objects.requireNonNull(lastException);
    throw new RuntimeException(
        String.format(
            "Local embedding call failed for model %s after %d attempts: %s",
            modelToUse, MAX_ATTEMPTS, cause.getMessage()),
        cause);
  }

  private RuntimeException newConnectError(Throwable cause) {
    return new RuntimeException(
        String.format(
            "Cannot connect to local embedding server at %s. "
                + "Is it running? Start Ollama with: ollama serve",
            endpoint),
        cause);
  }

  @Nonnull
  private float[] embedInternal(@Nonnull String text, @Nonnull String modelToUse)
      throws IOException, InterruptedException {
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("input", text);
    requestBody.put("model", modelToUse);
    requestBody.put("encoding_format", "float");

    String requestJson = objectMapper.writeValueAsString(requestBody);
    log.debug("Local embedding request for model {}: {}", modelToUse, requestJson);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .timeout(REQUEST_TIMEOUT)
            .header("Content-Type", "application/json")
            // Ollama ignores this header; included for compatibility with servers that require
            // a non-empty bearer token (e.g., LM Studio).
            .header("Authorization", "Bearer local")
            .POST(HttpRequest.BodyPublishers.ofString(requestJson))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      String errorMsg =
          String.format(
              "Local embedding server returned status %d for model '%s': %s",
              response.statusCode(), modelToUse, response.body());
      if (response.statusCode() == 404) {
        throw new RuntimeException(
            errorMsg + String.format(". Model not pulled? Try: ollama pull %s", modelToUse));
      }
      if (response.statusCode() >= 500) {
        // 5xx errors are transient — throw checked so the retry loop retries
        throw new IOException(errorMsg);
      }
      throw new RuntimeException(errorMsg);
    }

    // OpenAI-compatible format:
    // {"object":"list","data":[{"object":"embedding","embedding":[0.123,...],"index":0}],...}
    String responseJson = response.body();
    log.debug("Local embedding response: {}", responseJson);

    JsonNode responseNode = objectMapper.readTree(responseJson);
    JsonNode dataNode = responseNode.get("data");

    if (dataNode == null || !dataNode.isArray() || dataNode.size() == 0) {
      throw new RuntimeException(
          "Invalid response from local embedding server: missing or empty data array");
    }

    JsonNode embeddingObject = dataNode.get(0);
    JsonNode embeddingArray = embeddingObject.get("embedding");

    if (embeddingArray == null || !embeddingArray.isArray()) {
      throw new RuntimeException(
          "Invalid response from local embedding server: embedding is not an array");
    }

    int dimensions = embeddingArray.size();
    float[] embedding = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      JsonNode value = embeddingArray.get(i);
      if (value.isNumber()) {
        embedding[i] = (float) value.asDouble();
      } else {
        throw new RuntimeException(
            "Invalid response from local embedding server: embedding contains non-numeric value");
      }
    }

    log.debug("Generated embedding with {} dimensions for model {}", dimensions, modelToUse);
    return embedding;
  }
}
