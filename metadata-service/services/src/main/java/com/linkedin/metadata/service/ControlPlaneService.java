package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.metadata.config.ControlPlaneConfiguration;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for interacting with the DataHub Control Plane API.
 *
 * <p>This service provides methods to fetch trial expiration information and other control plane
 * data. Results are cached to reduce API load.
 *
 * <p>On API failures, the service returns the last cached value (if available) to ensure graceful
 * degradation.
 */
@Slf4j
public class ControlPlaneService {

  private static final String CACHE_KEY = "trial_expiration_timestamp";
  private static final int HTTP_TIMEOUT_SECONDS = 10;
  private static final long CACHE_TTL_DEFAULT_MINUTES = 15L;
  private static final String ADMIN_INVITE_TOKEN_ENDPOINT =
      "/api/instances/trials/admin-invite-token";
  private static final String TRIAL_EXPIRATION_ENDPOINT =
      "/api/instances/trials/expiration-timestamp";

  private final ControlPlaneConfiguration config;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final Cache<String, Long> cache;

  public ControlPlaneService(@Nonnull final ControlPlaneConfiguration config) {
    this(
        config,
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
            .build());
  }

  public ControlPlaneService(
      @Nonnull final ControlPlaneConfiguration config, @Nonnull final HttpClient httpClient) {
    this.config = config;
    this.httpClient = httpClient;
    this.objectMapper = new ObjectMapper();

    long cacheTtl =
        config.getCacheTtlMinutes() != null
            ? config.getCacheTtlMinutes()
            : CACHE_TTL_DEFAULT_MINUTES;
    this.cache = CacheBuilder.newBuilder().expireAfterWrite(cacheTtl, TimeUnit.MINUTES).build();

    log.info(
        "ControlPlaneService initialized with URL: {}, cache TTL: {} minutes",
        config.getUrl(),
        cacheTtl);
  }

  /**
   * Fetches the trial expiration timestamp.
   *
   * <p>This method makes a POST request to the control plane API to fetch the trial expiration
   * timestamp (Unix timestamp in seconds).
   *
   * <p>Results are cached based on the configured TTL. On API failures, the last cached value is
   * returned (graceful degradation).
   *
   * @return Optional containing the expiration timestamp (Unix seconds), or empty if not configured
   *     or no cached data available
   */
  @Nonnull
  public Optional<Long> getTrialExpirationTimestamp() {
    if (!isConfigured()) {
      log.debug("Control plane not configured, returning empty");
      return Optional.empty();
    }

    try {
      // Check cache first
      Long cachedValue = cache.getIfPresent(CACHE_KEY);
      if (cachedValue != null) {
        log.debug("Returning cached trial expiration timestamp: {}", cachedValue);
        return Optional.of(cachedValue);
      }

      // Fetch from control plane API
      log.debug("Fetching trial expiration from control plane: {}", config.getUrl());
      Optional<Long> result = fetchTrialExpirationTimestamp();

      if (result.isPresent()) {
        cache.put(CACHE_KEY, result.get());
        log.debug("Successfully fetched and cached trial expiration timestamp: {}", result.get());
        return result;
      } else {
        // On failure, return stale cache if available
        cachedValue = cache.getIfPresent(CACHE_KEY);
        if (cachedValue != null) {
          log.warn(
              "Failed to fetch from control plane, returning stale cached timestamp: {}",
              cachedValue);
          return Optional.of(cachedValue);
        }
        log.warn("Failed to fetch from control plane and no cached value available");
        return Optional.empty();
      }
    } catch (Exception e) {
      log.error("Unexpected error fetching trial expiration: {}", e.getMessage(), e);
      // On exception, try to return stale cache
      Long cachedValue = cache.getIfPresent(CACHE_KEY);
      if (cachedValue != null) {
        log.warn("Returning stale cached timestamp due to exception: {}", cachedValue);
        return Optional.of(cachedValue);
      }
      return Optional.empty();
    }
  }

  /**
   * Fetches the trial expiration timestamp from the control plane API.
   *
   * @return Optional containing the expiration timestamp (Unix seconds), or empty if fetch failed
   */
  @Nonnull
  private Optional<Long> fetchTrialExpirationTimestamp() {
    try {
      String endpoint =
          String.format(
              "%s%s?api_key=%s", config.getUrl(), TRIAL_EXPIRATION_ENDPOINT, config.getApiKey());

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(endpoint))
              .timeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
              .header("Content-Type", "application/json")
              .GET()
              .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        log.warn(
            "Control plane API returned non-200 status: {} for endpoint: {}",
            response.statusCode(),
            endpoint);
        return Optional.empty();
      }

      String body = response.body();
      if (body == null || body.trim().isEmpty()) {
        log.warn("Control plane API returned empty body");
        return Optional.empty();
      }

      // Parse response
      long expirationTimestamp = objectMapper.readTree(body).asLong();
      log.debug("Successfully parsed trial expiration timestamp: {}", expirationTimestamp);
      return Optional.of(expirationTimestamp);

    } catch (Exception e) {
      log.warn("Error fetching trial expiration from control plane: {}", e.getMessage());
      log.debug("Fetch error details", e);
      return Optional.empty();
    }
  }

  /**
   * Checks if the control plane is properly configured.
   *
   * @return true if both URL and API key are configured
   */
  public boolean isConfigured() {
    return config != null
        && config.getUrl() != null
        && !config.getUrl().trim().isEmpty()
        && config.getApiKey() != null
        && !config.getApiKey().trim().isEmpty();
  }

  /**
   * Sends the admin invite token to the control plane.
   *
   * <p>Uses generous retry logic with exponential backoff in case the control plane is
   * intermittently unavailable during startup.
   *
   * @param adminInviteToken the admin invite token to send
   * @param retryCount number of retry attempts
   * @param retryIntervalSeconds initial retry interval in seconds (doubles each retry)
   * @return true if successfully sent, false otherwise
   */
  @Nonnull
  public boolean sendAdminInviteToken(
      @Nonnull String adminInviteToken, int retryCount, int retryIntervalSeconds) {
    if (!isConfigured()) {
      log.warn("Control plane not configured, skipping admin invite token send");
      return false;
    }

    int attemptCount = 0;

    while (attemptCount < retryCount) {
      try {
        String endpoint = config.getUrl() + ADMIN_INVITE_TOKEN_ENDPOINT;
        String requestBody =
            objectMapper.writeValueAsString(
                Map.of("token", adminInviteToken, "api_key", config.getApiKey()));

        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .timeout(Duration.ofSeconds(HTTP_TIMEOUT_SECONDS))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response =
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
          log.info("Successfully sent admin invite token to control plane");
          return true;
        } else {
          log.warn(
              "Control plane returned non-200 status: {} for endpoint: {}",
              response.statusCode(),
              endpoint);
          if (attemptCount == retryCount - 1) {
            return false;
          }
        }
      } catch (Exception e) {
        log.warn(
            "Attempt {} failed to send admin invite token: {}", attemptCount + 1, e.getMessage());
        if (attemptCount == retryCount - 1) {
          log.error("Failed to send admin invite token after {} attempts", retryCount, e);
          return false;
        }
      }

      try {
        long backoffMs = calculateExponentialBackoff(attemptCount, retryIntervalSeconds);
        log.info("Retrying in {} ms (attempt {}/{})", backoffMs, attemptCount + 1, retryCount);
        Thread.sleep(backoffMs);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        log.error("Interrupted while waiting to retry", ie);
        return false;
      }

      attemptCount++;
    }

    return false;
  }

  /**
   * Compute exponential backoff delay.
   *
   * @param attemptCount the current attempt number (0-indexed)
   * @param baseIntervalSeconds base retry interval in seconds
   * @return backoff delay in milliseconds
   */
  private long calculateExponentialBackoff(int attemptCount, int baseIntervalSeconds) {
    return (long) (baseIntervalSeconds * Math.pow(2, attemptCount) * 1000);
  }

  /** Clears the cache. Useful for testing or forcing a refresh. */
  public void clearCache() {
    cache.invalidateAll();
    log.debug("Control plane cache cleared");
  }
}
