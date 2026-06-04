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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
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

  private static final String DEFAULT_FIELD = "_default";
  private static final String COHORTS_FIELD = "cohorts";
  private static final String VERSION_FIELD = "version";
  private static final String DEPLOYMENTS_FIELD = "deployments";
  private static final int FETCH_TIMEOUT_MS = 10_000;

  /**
   * Permissive cleanliness check for version strings — NOT a PEP 440 validator. The regex accepts
   * any non-empty string composed of alphanumerics, underscore, dot, plus, exclamation, and hyphen.
   * We deliberately do not parse semantic version structure (release segments, pre / post / dev
   * tags, local version identifier) because pip is the source of truth: any string that passes here
   * but isn't a real PyPI release surfaces a clear "No matching distribution" error from pip at
   * install time. This check exists so operator fat-fingering of the matrix file (pasting HTML,
   * whitespace, JSON) produces a structured WARN at fetch time rather than a cryptic pip error
   * minutes later on every execution.
   *
   * <p>Accepts every shape DataHub publishes to PyPI today:
   *
   * <ul>
   *   <li>{@code 1.5.0.5}, {@code 1.4.0.3} — standard release
   *   <li>{@code 1.5.0.6rc1} — release candidate
   *   <li>{@code 1.5.0.13.post1} — post-release
   *   <li>{@code 1!0.0.0.dev0} — PEP 440 epoch + dev tag
   *   <li>{@code acryl-1.6.0+acryl.20251031} — internal build with local version identifier
   * </ul>
   *
   * <p>Also accepts the special non-PyPI version sentinels the ingestion executor recognizes —
   * these are not PEP 440 / PyPI versions and must <em>not</em> be rejected. This list is not
   * exhaustive; the permissive character class is intentional so future sentinels keep working
   * without a code change here. Known examples:
   *
   * <ul>
   *   <li>{@code bundled} — run the CLI bundled in the executor image, skipping the pip install
   *   <li>{@code no-acryl-datahub} — opt out of installing acryl-datahub (the recipe pulls it in
   *       transitively)
   * </ul>
   *
   * <p>Rejects: whitespace, embedded JSON like {@code {"version":"…"}}, HTML fragments, URLs,
   * anything containing characters outside the allowed set.
   */
  private static final Pattern VALID_VERSION_PATTERN = Pattern.compile("^[\\w.+!-]+$");

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
          parsed = parseMatrix(root);
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

  /**
   * Parses the nested schema into an {@link IngestionCliVersionMatrix} with two layers of
   * validation:
   *
   * <ul>
   *   <li><b>File-level (fail closed)</b>: if the root isn't a JSON object, throws {@link
   *       IllegalArgumentException}. The caller refuses to swap the cache and the last-known-good
   *       matrix continues to serve resolutions. Prevents a malformed file from blanking everyone.
   *   <li><b>Entry-level (fail open + log)</b>: individual malformed connector entries / cohorts /
   *       version strings are skipped with structured WARN logs that name the offending path
   *       ({@code server='X' connector='Y' cohort[N]}). Good entries around the bad one are kept,
   *       so a single typo doesn't take the whole matrix down.
   * </ul>
   *
   * <p>Package-private so tests can drive it directly with arbitrary {@link JsonNode} input.
   */
  static IngestionCliVersionMatrix parseMatrix(JsonNode root) {
    if (root == null || !root.isObject()) {
      throw new IllegalArgumentException(
          "matrix root must be a JSON object, got: "
              + (root == null ? "null" : root.getNodeType().toString().toLowerCase()));
    }

    Map<String, ServerEntry> entries = new HashMap<>();
    root.fields()
        .forEachRemaining(
            serverEntry -> {
              String serverVersion = serverEntry.getKey();
              JsonNode serverValue = serverEntry.getValue();
              if (!serverValue.isObject()) {
                log.warn(
                    "Skipping malformed matrix entry: server='{}' value is not an object (got {})",
                    serverVersion,
                    serverValue.getNodeType().toString().toLowerCase());
                return;
              }
              Map<String, ConnectorEntry> connectors = new HashMap<>();
              serverValue
                  .fields()
                  .forEachRemaining(
                      connectorEntry -> {
                        String connector = connectorEntry.getKey();
                        ConnectorEntry parsed =
                            parseConnectorEntry(
                                serverVersion, connector, connectorEntry.getValue());
                        if (parsed != null) {
                          connectors.put(connector, parsed);
                        }
                      });
              entries.put(serverVersion, new ServerEntry(connectors));
            });
    return new IngestionCliVersionMatrix(entries);
  }

  /**
   * Parses a single connector entry. Returns {@code null} if the entry's structure is wholly
   * malformed (not a JSON object) — caller drops that connector. Bad sub-fields ({@code _default},
   * {@code cohorts}) are logged and either ignored or skipped while keeping the rest of the entry.
   */
  @Nullable
  private static ConnectorEntry parseConnectorEntry(
      String serverVersion, String connector, JsonNode connectorNode) {
    if (!connectorNode.isObject()) {
      log.warn(
          "Skipping malformed matrix entry: server='{}' connector='{}' value is not an object (got {})",
          serverVersion,
          connector,
          connectorNode.getNodeType().toString().toLowerCase());
      return null;
    }

    JsonNode defaultNode = connectorNode.path(DEFAULT_FIELD);
    String defaultVersion = null;
    if (!defaultNode.isMissingNode() && !defaultNode.isNull()) {
      if (!defaultNode.isTextual()) {
        log.warn(
            "Ignoring non-textual '_default' value: server='{}' connector='{}' (got {})",
            serverVersion,
            connector,
            defaultNode.getNodeType().toString().toLowerCase());
      } else {
        String candidate = defaultNode.asText();
        if (isValidVersion(candidate)) {
          defaultVersion = candidate;
        } else {
          log.warn(
              "Ignoring invalid '_default' version: server='{}' connector='{}' version='{}'. "
                  + "Cohort matches still apply; this connector will fall through to "
                  + "APPLICATION_DEFAULT when no cohort matches.",
              serverVersion,
              connector,
              candidate);
        }
      }
    }

    List<Cohort> cohorts = new ArrayList<>();
    JsonNode cohortsNode = connectorNode.path(COHORTS_FIELD);
    if (cohortsNode.isArray()) {
      int index = 0;
      for (JsonNode cohortNode : cohortsNode) {
        Cohort cohort = parseCohort(serverVersion, connector, index, cohortNode);
        if (cohort != null) {
          cohorts.add(cohort);
        }
        index++;
      }
    } else if (!cohortsNode.isMissingNode() && !cohortsNode.isNull()) {
      log.warn(
          "Skipping malformed 'cohorts' field: server='{}' connector='{}' cohorts is not an array (got {})",
          serverVersion,
          connector,
          cohortsNode.getNodeType().toString().toLowerCase());
    }
    return new ConnectorEntry(defaultVersion, cohorts);
  }

  /**
   * Parses a single cohort entry. Returns {@code null} if the cohort is unusable (missing or
   * invalid {@code version}). Other malformed sub-fields are logged but don't drop the cohort.
   */
  @Nullable
  private static Cohort parseCohort(
      String serverVersion, String connector, int cohortIndex, JsonNode cohortNode) {
    if (!cohortNode.isObject()) {
      log.warn(
          "Skipping malformed cohort: server='{}' connector='{}' cohort[{}] is not an object",
          serverVersion,
          connector,
          cohortIndex);
      return null;
    }

    JsonNode versionNode = cohortNode.path(VERSION_FIELD);
    if (versionNode.isMissingNode() || versionNode.isNull() || !versionNode.isTextual()) {
      log.warn(
          "Skipping malformed cohort: server='{}' connector='{}' cohort[{}] missing or non-textual 'version'",
          serverVersion,
          connector,
          cohortIndex);
      return null;
    }
    String version = versionNode.asText();
    if (!isValidVersion(version)) {
      log.warn(
          "Skipping malformed cohort: server='{}' connector='{}' cohort[{}] has invalid version '{}'",
          serverVersion,
          connector,
          cohortIndex,
          version);
      return null;
    }

    Set<String> deployments = new HashSet<>();
    JsonNode deploymentsNode = cohortNode.path(DEPLOYMENTS_FIELD);
    if (deploymentsNode.isArray()) {
      for (JsonNode d : deploymentsNode) {
        if (d.isTextual()) {
          deployments.add(d.asText());
        } else {
          log.warn(
              "Ignoring non-string deployment entry: server='{}' connector='{}' cohort[{}] (got {})",
              serverVersion,
              connector,
              cohortIndex,
              d.getNodeType().toString().toLowerCase());
        }
      }
    } else if (!deploymentsNode.isMissingNode() && !deploymentsNode.isNull()) {
      log.warn(
          "Ignoring malformed 'deployments' field: server='{}' connector='{}' cohort[{}] is not an array (got {})",
          serverVersion,
          connector,
          cohortIndex,
          deploymentsNode.getNodeType().toString().toLowerCase());
    }
    return new Cohort(version, deployments);
  }

  /**
   * Returns {@code true} if {@code s} is a non-empty string matching {@link
   * #VALID_VERSION_PATTERN}. Permissive — allows weird-but-valid PyPI versions like {@code
   * "1.5.0.6rc1"} or {@code "1!0.0.0.dev0"} — but rejects whitespace, embedded markup, and other
   * shapes that obviously aren't versions.
   */
  private static boolean isValidVersion(String s) {
    return s != null && !s.isEmpty() && VALID_VERSION_PATTERN.matcher(s).matches();
  }
}
