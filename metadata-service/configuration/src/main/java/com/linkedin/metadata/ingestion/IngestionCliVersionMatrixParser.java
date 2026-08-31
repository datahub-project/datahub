package com.linkedin.metadata.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Parses the nested per-connector ingestion CLI version matrix JSON into an {@link
 * IngestionCliVersionMatrix}.
 *
 * <p>Shared by every {@link IngestionCliVersionMatrixSource} implementation (HTTP, S3, …) so the
 * schema, validation rules, and version sanity-checking live in exactly one place rather than being
 * copied per backend.
 *
 * <p>Validation has two layers:
 *
 * <ul>
 *   <li><b>File-level (fail closed)</b>: if the root isn't a JSON object, {@link #parseMatrix}
 *       throws {@link IllegalArgumentException}. Callers refuse to swap their cache and the
 *       last-known-good matrix continues to serve resolutions. Prevents a malformed file from
 *       blanking everyone.
 *   <li><b>Entry-level (fail open + log)</b>: individual malformed connector entries / cohorts /
 *       version strings are skipped with structured WARN logs that name the offending path ({@code
 *       server='X' connector='Y' cohort[N]}). Good entries around the bad one are kept, so a single
 *       typo doesn't take the whole matrix down.
 * </ul>
 */
@Slf4j
public final class IngestionCliVersionMatrixParser {

  private static final String DEFAULT_FIELD = "_default";
  private static final String COHORTS_FIELD = "cohorts";
  private static final String VERSION_FIELD = "version";
  private static final String DEPLOYMENTS_FIELD = "deployments";

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

  private IngestionCliVersionMatrixParser() {}

  /**
   * Parses the nested schema into an {@link IngestionCliVersionMatrix}. See the class javadoc for
   * the two-layer (file-level fail-closed, entry-level fail-open + log) validation strategy.
   */
  public static IngestionCliVersionMatrix parseMatrix(JsonNode root) {
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
