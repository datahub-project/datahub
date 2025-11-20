package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for fetching product update information from a remote JSON source.
 *
 * <p>This service attempts to fetch from a remote URL and falls back to a bundled classpath
 * resource if the remote fetch fails. This ensures product updates work in both internet-connected
 * and air-gapped deployments.
 *
 * <p>The service uses an in-memory cache with a 6-hour TTL to reduce network requests.
 */
@Slf4j
public class ProductUpdateService {

  private static final String CACHE_KEY = "product_update_json";
  private static final long CACHE_TTL_HOURS = 6;
  private static final int HTTP_TIMEOUT_SECONDS = 10;

  private final String jsonUrl;
  private final String fallbackResource;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final Cache<String, JsonNode> cache;

  public ProductUpdateService(
      @Nullable final String jsonUrl, @Nonnull final String fallbackResource) {
    this(
        jsonUrl,
        fallbackResource,
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS)).build());
  }

  public ProductUpdateService(
      @Nullable final String jsonUrl,
      @Nonnull final String fallbackResource,
      @Nonnull final HttpClient httpClient) {
    this.jsonUrl = jsonUrl;
    this.fallbackResource = fallbackResource;
    this.httpClient = httpClient;
    this.objectMapper = new ObjectMapper();
    this.cache =
        CacheBuilder.newBuilder().expireAfterWrite(CACHE_TTL_HOURS, TimeUnit.HOURS).build();

    log.info(
        "ProductUpdateService initialized with URL: {}, fallback resource: {}",
        jsonUrl,
        fallbackResource);
  }

  /**
   * Fetches the latest product update JSON from the configured URL, falling back to classpath
   * resource if remote fetch fails.
   *
   * <p>This method gracefully handles all errors by returning a fallback from the classpath. This
   * ensures product updates work in both internet-connected and air-gapped deployments.
   *
   * <p>Remote results are cached for 6 hours to reduce network load.
   *
   * @return Optional containing the parsed JSON node from remote or fallback, empty Optional only
   *     if both fail
   */
  @Nonnull
  public Optional<JsonNode> getLatestProductUpdate() {
    // Try remote fetch if URL is configured
    if (jsonUrl != null && !jsonUrl.trim().isEmpty()) {
      try {
        // Check cache first
        JsonNode cachedValue = cache.getIfPresent(CACHE_KEY);
        if (cachedValue != null) {
          log.debug("Returning cached product update JSON");
          return Optional.of(cachedValue);
        }

        // Fetch from remote source
        log.debug("Fetching product update JSON from: {}", jsonUrl);
        JsonNode result = fetchFromRemote();

        if (result != null) {
          cache.put(CACHE_KEY, result);
          log.info("Successfully fetched and cached product update JSON from remote");
          return Optional.of(result);
        }
      } catch (Exception e) {
        log.warn(
            "Failed to fetch product update JSON from {}: {}, falling back to classpath resource",
            jsonUrl,
            e.getMessage());
        log.debug("Product update fetch error details", e);
      }
    } else {
      log.debug("Product update JSON URL not configured, using classpath fallback");
    }

    // Fallback to classpath resource
    return loadFromClasspath();
  }

  /**
   * Fetches the JSON from the remote URL.
   *
   * @return the parsed JSON node, or null if fetch failed
   */
  @Nullable
  private JsonNode fetchFromRemote() {
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(jsonUrl))
              .timeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
              .GET()
              .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        log.warn("Product update JSON fetch returned non-200 status: {}", response.statusCode());
        return null;
      }

      String body = response.body();
      if (body == null || body.trim().isEmpty()) {
        log.warn("Product update JSON fetch returned empty body");
        return null;
      }

      JsonNode jsonNode = objectMapper.readTree(body);
      log.info("Successfully parsed product update JSON: {}", jsonNode.toString());
      return jsonNode;

    } catch (Exception e) {
      log.warn("Error fetching product update JSON: {}", e.getMessage());
      log.debug("Fetch error details", e);
      return null;
    }
  }

  /**
   * Loads the product update JSON from the classpath.
   *
   * @return Optional containing the parsed JSON node, empty if resource not found or invalid
   */
  @Nonnull
  private Optional<JsonNode> loadFromClasspath() {
    try {
      log.debug("Loading product update JSON from classpath: {}", fallbackResource);
      var inputStream = getClass().getClassLoader().getResourceAsStream(fallbackResource);

      if (inputStream == null) {
        log.error("Fallback resource not found on classpath: {}", fallbackResource);
        return Optional.empty();
      }

      JsonNode jsonNode = objectMapper.readTree(inputStream);
      log.info("Successfully loaded product update JSON from classpath fallback");
      return Optional.of(jsonNode);

    } catch (Exception e) {
      log.error(
          "Failed to load product update JSON from classpath resource {}: {}",
          fallbackResource,
          e.getMessage(),
          e);
      return Optional.empty();
    }
  }

  /** Clears the cache. Useful for testing or forcing a refresh. */
  public void clearCache() {
    cache.invalidateAll();
    log.debug("Product update cache cleared");
  }
}
