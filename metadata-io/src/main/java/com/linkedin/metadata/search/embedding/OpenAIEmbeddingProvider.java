package com.linkedin.metadata.search.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
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
 * Implementation of {@link EmbeddingProvider} that calls OpenAI's Embeddings API to generate query
 * embeddings.
 *
 * <p>This provider uses Java's built-in HttpClient and supports:
 *
 * <ul>
 *   <li>OpenAI Cloud API (api.openai.com)
 *   <li>Azure OpenAI Service (custom endpoint)
 *   <li>OpenAI-compatible endpoints
 * </ul>
 *
 * <p>Supports all OpenAI embedding models including text-embedding-3-small (1536 dimensions),
 * text-embedding-3-large (3072 dimensions), and text-embedding-ada-002 (1536 dimensions).
 *
 * @see <a href="https://platform.openai.com/docs/api-reference/embeddings">OpenAI Embeddings
 *     API</a>
 */
@Slf4j
public class OpenAIEmbeddingProvider implements EmbeddingProvider {

  private static final String DEFAULT_MODEL = "text-embedding-3-small";
  private static final String DEFAULT_ENDPOINT = "https://api.openai.com/v1/embeddings";
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  private static final int MAX_ATTEMPTS = 2;

  private final String apiKey;
  private final String endpoint;
  @Nonnull private final String defaultModel;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  /**
   * Creates a new OpenAIEmbeddingProvider with default settings.
   *
   * @param apiKey OpenAI API key (starts with "sk-")
   */
  public OpenAIEmbeddingProvider(@Nonnull String apiKey) {
    this(apiKey, DEFAULT_ENDPOINT, DEFAULT_MODEL);
  }

  /**
   * Creates a new OpenAIEmbeddingProvider with custom configuration.
   *
   * @param apiKey OpenAI API key
   * @param endpoint Custom endpoint URL (e.g., for Azure OpenAI)
   * @param defaultModel Default embedding model (e.g., "text-embedding-3-small")
   */
  public OpenAIEmbeddingProvider(
      @Nonnull String apiKey, @Nonnull String endpoint, @Nonnull String defaultModel) {
    this(
        apiKey,
        endpoint,
        defaultModel,
        HttpClient.newBuilder()
            .connectTimeout(DEFAULT_TIMEOUT)
            .version(HttpClient.Version.HTTP_1_1)
            .build());
  }

  /**
   * Creates a provider with a custom HttpClient (useful for testing).
   *
   * @param apiKey OpenAI API key
   * @param endpoint Custom endpoint URL
   * @param defaultModel Default embedding model
   * @param httpClient Pre-configured HttpClient
   */
  public OpenAIEmbeddingProvider(
      @Nonnull String apiKey,
      @Nonnull String endpoint,
      @Nonnull String defaultModel,
      @Nonnull HttpClient httpClient) {
    this.apiKey = Objects.requireNonNull(apiKey, "apiKey cannot be null");
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
    this.defaultModel = Objects.requireNonNull(defaultModel, "defaultModel cannot be null");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient cannot be null");
    this.objectMapper = new ObjectMapper();

    log.info(
        "Initialized OpenAIEmbeddingProvider with endpoint={}, model={}", endpoint, defaultModel);
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
      } catch (RuntimeException e) {
        // Non-retryable: bad API key, malformed response, 4xx errors, etc.
        lastException = e;
        break;
      } catch (Exception e) {
        lastException = e;
        if (attempt < MAX_ATTEMPTS) {
          log.warn(
              "OpenAI embedding attempt {}/{} failed for model {}, retrying: {}",
              attempt,
              MAX_ATTEMPTS,
              modelToUse,
              e.getMessage());
        }
      }
    }

    log.error(
        "All {} attempts failed for OpenAI embedding with model {}", MAX_ATTEMPTS, modelToUse);
    Exception cause = Objects.requireNonNull(lastException);
    throw new RuntimeException(
        String.format(
            "OpenAI API call failed for model %s after %d attempts: %s",
            modelToUse, MAX_ATTEMPTS, cause.getMessage()),
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
    log.debug("OpenAI request for model {}: {}", modelToUse, requestJson);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .timeout(DEFAULT_TIMEOUT)
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer " + apiKey)
            .POST(HttpRequest.BodyPublishers.ofString(requestJson))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      String errorMsg =
          String.format(
              "OpenAI API returned status %d for model %s: %s",
              response.statusCode(), modelToUse, response.body());
      if (response.statusCode() >= 500) {
        // 5xx errors are transient server-side failures — throw checked so the retry loop retries
        throw new IOException(errorMsg);
      }
      throw new RuntimeException(errorMsg);
    }

    // Format: {"object": "list", "data": [{"object": "embedding", "embedding": [0.123, ...],
    // "index": 0}], "model": "...", "usage": {...}}
    String responseJson = response.body();
    log.debug("OpenAI response: {}", responseJson);

    JsonNode responseNode = objectMapper.readTree(responseJson);
    JsonNode dataNode = responseNode.get("data");

    if (dataNode == null || !dataNode.isArray() || dataNode.size() == 0) {
      throw new RuntimeException("Invalid response from OpenAI: missing or empty data array");
    }

    JsonNode embeddingObject = dataNode.get(0);
    JsonNode embeddingArray = embeddingObject.get("embedding");

    if (embeddingArray == null || !embeddingArray.isArray()) {
      throw new RuntimeException("Invalid response from OpenAI: embedding is not an array");
    }

    int dimensions = embeddingArray.size();
    float[] embedding = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      JsonNode value = embeddingArray.get(i);
      if (value.isNumber()) {
        embedding[i] = (float) value.asDouble();
      } else {
        throw new RuntimeException(
            "Invalid response from OpenAI: embedding contains non-numeric value");
      }
    }

    log.debug("Generated embedding with {} dimensions for model {}", dimensions, modelToUse);
    return embedding;
  }
}
