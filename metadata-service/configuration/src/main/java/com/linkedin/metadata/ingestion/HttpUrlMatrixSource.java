package com.linkedin.metadata.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
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
 * {@link MatrixSource} backed by a publicly-readable HTTP URL serving the matrix JSON. Suitable for
 * any deployment that wants to fetch the matrix from a CDN, object store (S3, GCS), or a GitHub raw
 * URL without rebuilding or redeploying GMS to change connector versions.
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
public class HttpUrlMatrixSource implements MatrixSource {

  private static final String DEFAULT_FIELD = "_default";
  private static final String COHORTS_FIELD = "cohorts";
  private static final String VERSION_FIELD = "version";
  private static final String DEPLOYMENTS_FIELD = "deployments";
  private static final int FETCH_TIMEOUT_MS = 10_000;

  /**
   * Basic version-string shape — alphanumeric, underscore, dot, plus, exclamation, hyphen. Catches
   * obvious typos (whitespace, embedded JSON, HTML, etc.) without trying to validate PEP 440 fully.
   * pip will catch any version that doesn't actually exist on PyPI; this check exists so operators
   * fat-fingering the matrix file get a clean WARN at fetch time rather than a cryptic pip error
   * minutes later on every execution.
   */
  private static final Pattern VALID_VERSION_PATTERN = Pattern.compile("^[\\w.+!-]+$");

  private final String url;
  @Nullable private final String authHeader;
  private final AtomicReference<Matrix> cached;
  private final AtomicLong lastFetchedAtMillis;
  private final ObjectMapper objectMapper;

  /** Convenience constructor for unauthenticated (public) URLs. */
  public HttpUrlMatrixSource(String url, int refreshIntervalSeconds) {
    this(url, refreshIntervalSeconds, null);
  }

  public HttpUrlMatrixSource(String url, int refreshIntervalSeconds, @Nullable String authHeader) {
    this.url = url;
    this.authHeader = authHeader;
    this.cached = new AtomicReference<>(Matrix.EMPTY);
    this.lastFetchedAtMillis = new AtomicLong(0L);
    this.objectMapper = new ObjectMapper();

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    // Fetch immediately on startup (delay=0), then repeat on the configured interval.
    executor.scheduleAtFixedRate(this::refresh, 0, refreshIntervalSeconds, TimeUnit.SECONDS);
  }

  @Override
  public Matrix getMatrix() {
    return cached.get();
  }

  @Override
  public long getLastFetchedAtMillis() {
    return lastFetchedAtMillis.get();
  }

  /** Package-private so tests can force a refresh without waiting for the scheduled tick. */
  void refresh() {
    try {
      URL connectionUrl = new URL(url);
      URLConnection conn = connectionUrl.openConnection();
      conn.setConnectTimeout(FETCH_TIMEOUT_MS);
      conn.setReadTimeout(FETCH_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "DataHub-GMS");
      if (authHeader != null && !authHeader.isEmpty()) {
        conn.setRequestProperty("Authorization", authHeader);
      }

      try (InputStream is = conn.getInputStream()) {
        JsonNode root = objectMapper.readTree(is);
        Matrix parsed;
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
    } catch (Exception e) {
      log.warn(
          "Failed to refresh ingestion version matrix from {}. Retaining last known matrix.",
          url,
          e);
    }
  }

  /**
   * Parses the nested schema into a {@link Matrix} with two layers of validation:
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
  static Matrix parseMatrix(JsonNode root) {
    if (root == null || !root.isObject()) {
      throw new IllegalArgumentException(
          "matrix root must be a JSON object, got: "
              + (root == null ? "null" : root.getNodeType().toString().toLowerCase()));
    }

    Map<String, Map<String, ConnectorEntry>> entries = new HashMap<>();
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
              entries.put(serverVersion, Collections.unmodifiableMap(connectors));
            });
    return new Matrix(entries);
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
                  + "WORKSPACE_DEFAULT when no cohort matches.",
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
