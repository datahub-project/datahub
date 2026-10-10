package com.linkedin.metadata.search.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link EmbeddingProvider} that calls the an OAuth2-authenticated
 * platform-agnostic embedding endpoint ({@code POST /platform/{platform}/model/{model}/embedding})
 * to generate query embeddings.
 *
 * <p>The AI gateway fronts multiple upstream LLM platforms (Google Vertex AI, Azure OpenAI, Amazon
 * Bedrock) behind a single contract, so this provider is platform-agnostic: the {@code platform}
 * and {@code model} are just routing parameters, not separate client implementations.
 *
 * <p>Authentication is JWT via AWS Cognito's {@code client_credentials} grant. Access tokens are
 * cached in memory and refreshed shortly before expiry so a normal embed call does not pay the cost
 * of a token round-trip; a 401/403 response also forces one token refresh + retry in case the
 * cached token was revoked early.
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.4">AI Gateway API Guide</a>
 */
@Slf4j
public class AiGatewayEmbeddingProvider implements EmbeddingProvider {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  private static final int MAX_ATTEMPTS = 2;
  // Refresh ahead of actual expiry so an in-flight request never races a token that dies mid-call.
  private static final Duration TOKEN_EXPIRY_BUFFER = Duration.ofSeconds(30);
  private static final String EMBEDDING_CONTENT_TYPE =
      "application/vnd.ai.gateway.text.embedding.v1+json";

  private final String baseUrl;
  private final String platform;
  @Nonnull private final String defaultModel;
  private final String tokenUrl;
  private final String clientId;
  private final String clientSecret;
  private final int dimensions;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final AtomicReference<CachedToken> cachedToken = new AtomicReference<>();

  /** Creates a new AiGatewayEmbeddingProvider with default HTTP client settings. */
  public AiGatewayEmbeddingProvider(
      @Nonnull String baseUrl,
      @Nonnull String platform,
      @Nonnull String defaultModel,
      @Nonnull String tokenUrl,
      @Nonnull String clientId,
      @Nonnull String clientSecret,
      int dimensions) {
    this(
        baseUrl,
        platform,
        defaultModel,
        tokenUrl,
        clientId,
        clientSecret,
        dimensions,
        HttpClient.newBuilder()
            .connectTimeout(DEFAULT_TIMEOUT)
            .version(HttpClient.Version.HTTP_1_1)
            .build());
  }

  /** Package-private constructor that accepts a custom {@link HttpClient} for testing. */
  AiGatewayEmbeddingProvider(
      @Nonnull String baseUrl,
      @Nonnull String platform,
      @Nonnull String defaultModel,
      @Nonnull String tokenUrl,
      @Nonnull String clientId,
      @Nonnull String clientSecret,
      int dimensions,
      @Nonnull HttpClient httpClient) {
    // Strip a trailing slash so URL building below never produces a double slash before
    // "/platform".
    String trimmedBaseUrl = Objects.requireNonNull(baseUrl, "baseUrl cannot be null").trim();
    this.baseUrl =
        trimmedBaseUrl.endsWith("/")
            ? trimmedBaseUrl.substring(0, trimmedBaseUrl.length() - 1)
            : trimmedBaseUrl;
    this.platform = Objects.requireNonNull(platform, "platform cannot be null");
    this.defaultModel = Objects.requireNonNull(defaultModel, "defaultModel cannot be null");
    this.tokenUrl = Objects.requireNonNull(tokenUrl, "tokenUrl cannot be null");
    this.clientId = Objects.requireNonNull(clientId, "clientId cannot be null");
    this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
    this.dimensions = dimensions;
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient cannot be null");
    this.objectMapper = new ObjectMapper();

    log.info(
        "Initialized AiGatewayEmbeddingProvider with baseUrl={}, platform={}, model={}, dimensions={}",
        this.baseUrl,
        platform,
        defaultModel,
        dimensions);
  }

  @Override
  @Nonnull
  public float[] embed(@Nonnull String text, @Nullable String model) {
    Objects.requireNonNull(text, "text cannot be null");

    @Nonnull String modelToUse = model != null ? model : defaultModel;
    Exception lastException = null;

    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        return embedInternal(text, modelToUse, attempt);
      } catch (AuthFailureException e) {
        // Cached token may have been revoked server-side before its advertised expiry; drop it so
        // the next attempt fetches a fresh one instead of retrying with the same stale credential.
        lastException = e;
        cachedToken.set(null);
        if (attempt >= MAX_ATTEMPTS) {
          break;
        }
        log.warn(
            "AI Gateway auth failure on attempt {}/{}, refreshing token", attempt, MAX_ATTEMPTS);
      } catch (RuntimeException e) {
        lastException = e;
        break;
      } catch (Exception e) {
        lastException = e;
        if (attempt < MAX_ATTEMPTS) {
          log.warn(
              "AI Gateway embedding attempt {}/{} failed for model {}, retrying: {}",
              attempt,
              MAX_ATTEMPTS,
              modelToUse,
              e.getMessage());
        }
      }
    }

    log.error(
        "All {} attempts failed for AI Gateway embedding with model {}", MAX_ATTEMPTS, modelToUse);
    Exception cause = Objects.requireNonNull(lastException);
    throw new RuntimeException(
        String.format(
            "AI Gateway embedding call failed for platform=%s model=%s after %d attempts: %s",
            platform, modelToUse, MAX_ATTEMPTS, cause.getMessage()),
        cause);
  }

  @Nonnull
  private float[] embedInternal(@Nonnull String text, @Nonnull String modelToUse, int attempt)
      throws IOException, InterruptedException {
    String accessToken = getAccessToken();

    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("data", text);
    if (dimensions > 0) {
      requestBody.putObject("options").put("dimensions", dimensions);
    }
    String requestJson = objectMapper.writeValueAsString(requestBody);

    URI uri =
        URI.create(
            String.format("%s/platform/%s/model/%s/embedding", baseUrl, platform, modelToUse));
    log.debug("AI Gateway embedding request (attempt {}) to {}: {}", attempt, uri, requestJson);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(uri)
            .timeout(DEFAULT_TIMEOUT)
            .header("Content-Type", EMBEDDING_CONTENT_TYPE)
            .header("Authorization", "Bearer " + accessToken)
            .POST(HttpRequest.BodyPublishers.ofString(requestJson))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    int status = response.statusCode();

    if (status == 401 || status == 403) {
      throw new AuthFailureException(
          String.format(
              "AI Gateway auth failure (%d) for platform=%s model=%s",
              status, platform, modelToUse));
    }

    if (status != 200) {
      String errorMsg =
          String.format(
              "AI Gateway returned status %d for platform=%s model=%s: %s",
              status, platform, modelToUse, response.body());
      if (status == 429 || status >= 500) {
        // Transient — throw checked so the retry loop in embed() retries.
        throw new IOException(errorMsg);
      }
      throw new RuntimeException(errorMsg);
    }

    return parseResponse(response.body(), modelToUse);
  }

  @Nonnull
  private float[] parseResponse(String responseJson, String modelToUse) throws IOException {
    log.debug("AI Gateway response: {}", responseJson);
    JsonNode responseNode = objectMapper.readTree(responseJson);
    JsonNode embeddingsNode = responseNode.get("embeddings");

    if (embeddingsNode == null || !embeddingsNode.isArray() || embeddingsNode.isEmpty()) {
      throw new RuntimeException(
          "Invalid response from AI Gateway: missing or empty 'embeddings' array for model "
              + modelToUse);
    }

    if (embeddingsNode.get(0) != null && embeddingsNode.get(0).isArray()) {
      embeddingsNode = embeddingsNode.get(0);
    }

    int size = embeddingsNode.size();
    float[] embedding = new float[size];
    for (int i = 0; i < size; i++) {
      JsonNode value = embeddingsNode.get(i);
      if (!value.isNumber()) {
        throw new RuntimeException(
            "Invalid response from AI Gateway: embeddings contains non-numeric value");
      }
      embedding[i] = (float) value.asDouble();
    }
    return embedding;
  }

  /**
   * Returns a cached Cognito access token, fetching a new one via the {@code client_credentials}
   * grant when missing or within {@link #TOKEN_EXPIRY_BUFFER} of expiry.
   */
  private synchronized String getAccessToken() throws IOException, InterruptedException {
    CachedToken existing = cachedToken.get();
    if (existing != null && Instant.now().isBefore(existing.expiresAt)) {
      return existing.accessToken;
    }

    String basicAuth =
        Base64.getEncoder()
            .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(tokenUrl))
            .timeout(DEFAULT_TIMEOUT)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Authorization", "Basic " + basicAuth)
            .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials"))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new RuntimeException(
          String.format(
              "Cognito token request failed with status %d: %s",
              response.statusCode(), response.body()));
    }

    JsonNode tokenNode = objectMapper.readTree(response.body());
    String accessToken = tokenNode.path("access_token").asText(null);
    if (accessToken == null || accessToken.isBlank()) {
      throw new RuntimeException("Cognito token response did not contain an 'access_token' field");
    }
    // Cognito's app client tokens are typically 1 hour; fall back conservatively if omitted.
    long expiresInSeconds = tokenNode.path("expires_in").asLong(3600);

    CachedToken fresh =
        new CachedToken(
            accessToken, Instant.now().plusSeconds(expiresInSeconds).minus(TOKEN_EXPIRY_BUFFER));
    cachedToken.set(fresh);
    return accessToken;
  }

  private static final class CachedToken {
    private final String accessToken;
    private final Instant expiresAt;

    private CachedToken(String accessToken, Instant expiresAt) {
      this.accessToken = accessToken;
      this.expiresAt = expiresAt;
    }
  }

  /** Signals a 401/403 from the AI Gateway so the retry loop can force a token refresh. */
  private static final class AuthFailureException extends RuntimeException {
    private AuthFailureException(String message) {
      super(message);
    }
  }
}
