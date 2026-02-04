package com.linkedin.metadata.search.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
 * Implementation of {@link EmbeddingProvider} that calls Cohere's Embed API to generate query
 * embeddings.
 *
 * <p>This provider uses Java's built-in HttpClient and supports:
 *
 * <ul>
 *   <li>Cohere Cloud API (api.cohere.ai)
 *   <li>Custom Cohere-compatible endpoints
 * </ul>
 *
 * <p>Supports all Cohere embedding models including embed-english-v3.0 (1024 dimensions),
 * embed-multilingual-v3.0 (1024 dimensions), and embed-english-light-v3.0 (384 dimensions).
 *
 * <p>Always uses "search_query" as the input_type for query embeddings, which is appropriate for
 * DataHub's semantic search use case.
 *
 * @see <a href="https://docs.cohere.com/reference/embed">Cohere Embed API</a>
 */
@Slf4j
public class CohereEmbeddingProvider implements EmbeddingProvider {

  private static final String DEFAULT_MODEL = "embed-english-v3.0";
  private static final String DEFAULT_ENDPOINT = "https://api.cohere.ai/v1/embed";
  private static final String INPUT_TYPE = "search_query";
  private static final String COHERE_VERSION = "2024-01-01";
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

  private final String apiKey;
  private final String endpoint;
  private final String defaultModel;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  /**
   * Creates a new CohereEmbeddingProvider with default settings.
   *
   * @param apiKey Cohere API key
   */
  public CohereEmbeddingProvider(@Nonnull String apiKey) {
    this(apiKey, DEFAULT_ENDPOINT, DEFAULT_MODEL);
  }

  /**
   * Creates a new CohereEmbeddingProvider with custom configuration.
   *
   * @param apiKey Cohere API key
   * @param endpoint Custom endpoint URL (e.g., for private deployments)
   * @param defaultModel Default embedding model (e.g., "embed-english-v3.0")
   */
  public CohereEmbeddingProvider(
      @Nonnull String apiKey, @Nonnull String endpoint, @Nonnull String defaultModel) {
    this(
        apiKey,
        endpoint,
        defaultModel,
        HttpClient.newBuilder().connectTimeout(DEFAULT_TIMEOUT).build());
  }

  /**
   * Creates a provider with a custom HttpClient (useful for testing).
   *
   * @param apiKey Cohere API key
   * @param endpoint Custom endpoint URL
   * @param defaultModel Default embedding model
   * @param httpClient Pre-configured HttpClient
   */
  public CohereEmbeddingProvider(
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
        "Initialized CohereEmbeddingProvider with endpoint={}, model={}", endpoint, defaultModel);
  }

  @Override
  @Nonnull
  public float[] embed(@Nonnull String text, @Nullable String model) {
    Objects.requireNonNull(text, "text cannot be null");

    String modelToUse = model != null ? model : defaultModel;

    try {
      // Build request JSON for Cohere Embed API
      // Format: {"texts": ["text"], "model": "embed-english-v3.0", "input_type": "search_query",
      // "truncate": "END"}
      ObjectNode requestBody = objectMapper.createObjectNode();

      // texts: array with single text (we're embedding one query at a time)
      ArrayNode textsArray = objectMapper.createArrayNode();
      textsArray.add(text);
      requestBody.set("texts", textsArray);

      requestBody.put("model", modelToUse);
      requestBody.put("input_type", INPUT_TYPE);
      requestBody.put("truncate", "END");

      String requestJson = objectMapper.writeValueAsString(requestBody);
      log.debug("Cohere request for model {}: {}", modelToUse, requestJson);

      // Build HTTP request
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(endpoint))
              .timeout(DEFAULT_TIMEOUT)
              .header("Content-Type", "application/json")
              .header("Authorization", "Bearer " + apiKey)
              .header("Cohere-Version", COHERE_VERSION)
              .POST(HttpRequest.BodyPublishers.ofString(requestJson))
              .build();

      // Send request
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      // Check response status
      if (response.statusCode() != 200) {
        String errorMsg =
            String.format(
                "Cohere API returned status %d for model %s: %s",
                response.statusCode(), modelToUse, response.body());
        log.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }

      // Parse response
      // Format: {"embeddings": [[0.123, 0.456, ...]], "id": "...", "response_type":
      // "embeddings_floats", "texts": ["..."]}
      String responseJson = response.body();
      log.debug("Cohere response: {}", responseJson);

      JsonNode responseNode = objectMapper.readTree(responseJson);
      JsonNode embeddingsNode = responseNode.get("embeddings");

      if (embeddingsNode == null || !embeddingsNode.isArray() || embeddingsNode.size() == 0) {
        throw new RuntimeException(
            "Invalid response from Cohere: missing or empty embeddings array");
      }

      // Extract first (and only) embedding
      JsonNode embeddingArray = embeddingsNode.get(0);
      if (!embeddingArray.isArray()) {
        throw new RuntimeException("Invalid response from Cohere: embedding is not an array");
      }

      // Convert to float[]
      int dimensions = embeddingArray.size();
      float[] embedding = new float[dimensions];
      for (int i = 0; i < dimensions; i++) {
        JsonNode value = embeddingArray.get(i);
        if (value.isNumber()) {
          embedding[i] = (float) value.asDouble();
        } else {
          throw new RuntimeException(
              "Invalid response from Cohere: embedding contains non-numeric value");
        }
      }

      log.debug("Generated embedding with {} dimensions for model {}", dimensions, modelToUse);
      return embedding;

    } catch (IOException e) {
      String errorMsg =
          String.format(
              "Failed to generate embedding with model %s: %s", modelToUse, e.getMessage());
      log.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      String errorMsg =
          String.format("Request interrupted for model %s: %s", modelToUse, e.getMessage());
      log.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    } catch (Exception e) {
      String errorMsg =
          String.format("Cohere API call failed for model %s: %s", modelToUse, e.getMessage());
      log.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }
}
