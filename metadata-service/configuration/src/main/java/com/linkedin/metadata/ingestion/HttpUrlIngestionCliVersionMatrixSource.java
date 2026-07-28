package com.linkedin.metadata.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IngestionCliVersionMatrixSource} backed by a publicly-readable HTTP / HTTPS URL serving
 * the matrix JSON. Suitable for any deployment that wants to fetch the matrix from a CDN, object
 * store (S3, GCS), or a GitHub raw URL without rebuilding or redeploying GMS to change connector
 * versions. Uses {@link java.net.http.HttpClient}, which is HTTP/HTTPS-only by design — non-HTTP
 * schemes ({@code file://}, {@code jar://}, {@code ftp://}, …) are rejected at request-send time.
 *
 * <p>The remote JSON must follow this schema:
 *
 * <pre>{@code
 * {
 *   "1.3.1.4": {
 *     "snowflake": {
 *       "_default": "1.3.1.4",
 *       "cohorts": [
 *         { "version": "1.3.1.5", "deployments": ["deployment-1", "deployment-2"] }
 *       ]
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>The cache is refreshed in the background on a configurable interval. On fetch failure the last
 * successfully loaded matrix is retained so in-flight executions never see a flapping view.
 *
 * <p>Reads from {@link #getMatrix()} are lock-free (single volatile read on {@link
 * AtomicReference#get()}); the background refresh swaps the cache atomically after a successful
 * fetch.
 *
 * <p>For private hosts (e.g. a private GitHub repo's {@code raw.githubusercontent.com} URL) an
 * optional {@code authHeader} value is sent verbatim as the {@code Authorization} request header.
 * Format is whatever the host expects — e.g. {@code "token ghp_xxx"} for a GitHub PAT, {@code
 * "Bearer ey..."} for an OIDC token. When {@code authHeader} is {@code null} or empty no auth
 * header is sent (the public-URL path is unchanged).
 */
@Slf4j
public class HttpUrlIngestionCliVersionMatrixSource implements IngestionCliVersionMatrixSource {

  private static final int FETCH_TIMEOUT_MS = 10_000;

  /** Seconds to wait for the refresh thread to drain on graceful shutdown. */
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

  /**
   * Thread name for the background refresh worker. Named so it stands out in thread dumps (the
   * default {@code Executors.defaultThreadFactory()} would produce {@code pool-N-thread-1}, which
   * gives an operator triaging a hung pod no idea what the thread does).
   */
  private static final String REFRESH_THREAD_NAME = "ingestion-cli-version-matrix-refresh";

  private final String url;
  @Nullable private final String authHeader;
  private final AtomicReference<IngestionCliVersionMatrix> cached;
  private final AtomicLong lastFetchedAtMillis;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;

  /** Background refresh scheduler, stopped by {@link #shutdown()} on Spring context teardown. */
  private final ScheduledExecutorService executor;

  /** Convenience constructor for unauthenticated (public) URLs. */
  public HttpUrlIngestionCliVersionMatrixSource(String url, int refreshIntervalSeconds) {
    this(url, refreshIntervalSeconds, null);
  }

  public HttpUrlIngestionCliVersionMatrixSource(
      String url, int refreshIntervalSeconds, @Nullable String authHeader) {
    this.url = url;
    this.authHeader = authHeader;
    this.cached = new AtomicReference<>(IngestionCliVersionMatrix.EMPTY);
    this.lastFetchedAtMillis = new AtomicLong(0L);
    this.objectMapper = new ObjectMapper();
    this.httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.ofMillis(FETCH_TIMEOUT_MS)).build();

    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, REFRESH_THREAD_NAME);
              // Daemon so the JVM can still exit cleanly if @PreDestroy somehow doesn't fire
              // (kill -9, container-runtime quirks). PreDestroy remains the primary shutdown path.
              t.setDaemon(true);
              return t;
            });
    // Fetch immediately on startup (delay=0), then repeat on the configured interval.
    this.executor.scheduleAtFixedRate(this::refresh, 0, refreshIntervalSeconds, TimeUnit.SECONDS);
  }

  /**
   * Gracefully stop the background refresh on Spring context teardown. Spring invokes this hook
   * during bean destruction so the scheduled-executor thread does not leak across context restarts
   * (relevant in dev hot-reload and integration-test contexts that re-create the bean).
   */
  @PreDestroy
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        // Refresh in progress took longer than the timeout — interrupt it. If a fetch was mid-IO
        // the connection will close, and parseMatrix is purely in-memory so it can't dangle.
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public IngestionCliVersionMatrix getMatrix() {
    return cached.get();
  }

  @Override
  public long getLastFetchedAtMillis() {
    return lastFetchedAtMillis.get();
  }

  /** Package-private so tests can force a refresh without waiting for the scheduled tick. */
  void refresh() {
    try {
      HttpRequest.Builder reqBuilder =
          HttpRequest.newBuilder(URI.create(url))
              .timeout(Duration.ofMillis(FETCH_TIMEOUT_MS))
              .header("User-Agent", "DataHub-GMS")
              // Lets the URL point at the GitHub "contents" API
              // (https://api.github.com/repos/<org>/<repo>/contents/<path>?ref=<branch>) — the only
              // authenticated way to read a file from a private/internal GitHub repo, since
              // raw.githubusercontent.com does not honor the Authorization header for those. With
              // this Accept the contents API returns the raw file body instead of base64 JSON.
              // Plain file hosts (raw URLs, gists, S3, CDNs) ignore an unknown Accept and still
              // return the file, so sending it unconditionally is safe for public URLs too.
              .header("Accept", "application/vnd.github.raw")
              .GET();
      if (authHeader != null && !authHeader.isEmpty()) {
        reqBuilder.header("Authorization", authHeader);
      }
      // HttpClient enforces HTTP/HTTPS at send-time — non-HTTP schemes (file://, jar://, ftp://,
      // …) throw IllegalArgumentException, which the outer catch turns into the same retain-cache
      // WARN we already emit for other fetch failures.
      HttpResponse<InputStream> response =
          httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofInputStream());

      if (response.statusCode() / 100 != 2) {
        log.warn(
            "Non-2xx response fetching ingestion version matrix from {}: HTTP {}. Retaining last known matrix.",
            url,
            response.statusCode());
        return;
      }

      try (InputStream is = response.body()) {
        JsonNode root = objectMapper.readTree(is);
        IngestionCliVersionMatrix parsed;
        try {
          parsed = IngestionCliVersionMatrixParser.parseMatrix(root);
        } catch (IllegalArgumentException schemaError) {
          // File-level schema violation (e.g. root not a JSON object). Refuse to swap the cache
          // — operator gets a fix-this-now WARN while in-flight resolutions continue to use the
          // last-known-good matrix. Per-entry violations are handled inside parseMatrix without
          // throwing; this branch is only for whole-file rejection.
          log.warn(
              "Refusing to swap matrix cache from {}: {}. Retaining last known matrix.",
              url,
              schemaError.getMessage());
          return;
        }
        cached.set(parsed);
        // Stamp the successful-fetch timestamp after the cache swap so readers never see a fresh
        // timestamp paired with a stale matrix.
        lastFetchedAtMillis.set(System.currentTimeMillis());
        log.info(
            "Successfully refreshed ingestion version matrix from {}; {} server version entries loaded",
            url,
            parsed.size());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(
          "Interrupted while refreshing ingestion version matrix from {}. Retaining last known matrix.",
          url);
    } catch (Exception e) {
      log.warn(
          "Failed to refresh ingestion version matrix from {}. Retaining last known matrix.",
          url,
          e);
    }
  }
}
