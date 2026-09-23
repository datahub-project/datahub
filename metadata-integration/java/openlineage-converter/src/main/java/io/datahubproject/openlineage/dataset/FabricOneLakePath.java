package io.datahubproject.openlineage.dataset;

import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps Microsoft Fabric OneLake table paths to the dataset names emitted by the {@code
 * fabric-onelake} ingestion source, so lineage captured at runtime (OpenLineage from Fabric Spark
 * notebooks / Spark job definitions) lands on the same URNs the connector ingests.
 *
 * <p>OneLake exposes the ADLS Gen2 ABFS (and Blob) APIs. A Lakehouse table is addressed as:
 *
 * <pre>
 *   abfss://{workspaceGUID}@onelake.dfs.fabric.microsoft.com/{itemGUID}/Tables/[{schema}/]{table}
 *   abfss://{workspaceName}@onelake.dfs.fabric.microsoft.com/{itemName}.{ItemType}/Tables/[{schema}/]{table}
 * </pre>
 *
 * The connector names datasets {@code {workspaceGUID}.{itemGUID}.{schema}.{table}} (schema {@code
 * dbo} for schemas-disabled Lakehouses), see {@code make_onelake_urn} in {@code
 * datahub.ingestion.source.fabric.common.urn_generator}. GUID-form paths are mapped directly.
 * Friendly-name paths carry no GUIDs, so they are only mapped when an explicit {@code
 * workspaceName/itemName.ItemType -> workspaceGUID/itemGUID} mapping is configured; otherwise they
 * are left to the default {@code abs} handling. Paths outside {@code /Tables/} (e.g. {@code
 * /Files/}) are not tables and are never mapped.
 */
@Slf4j
public final class FabricOneLakePath {

  public static final String PLATFORM = "fabric-onelake";
  public static final String DEFAULT_SCHEMA = "dbo";
  private static final String TABLES_FOLDER = "Tables";

  private static final List<String> SCHEMES = Arrays.asList("abfs", "abfss", "wasb", "wasbs");

  /**
   * OneLake endpoints: global ({@code onelake.dfs|blob.fabric.microsoft.com}), regional ({@code
   * <region>-onelake.dfs|blob.fabric.microsoft.com}), the general API FQDN ({@code
   * [<region>-]api.onelake.fabric.microsoft.com}) and the workspace private-link FQDN ({@code
   * <workspaceIdWithoutDashes>.z<xy>.dfs|blob|onelake.fabric.microsoft.com}).
   */
  private static final Pattern ONELAKE_HOST =
      Pattern.compile(
          "^(?:(?:[a-z0-9]+-)?onelake\\.(?:dfs|blob)"
              + "|(?:[a-z0-9]+-)?api\\.onelake"
              + "|[0-9a-f]{32}\\.z[0-9a-f]{2}\\.(?:dfs|blob|onelake))"
              + "\\.fabric\\.microsoft\\.com$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern GUID =
      Pattern.compile(
          "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          Pattern.CASE_INSENSITIVE);

  private FabricOneLakePath() {}

  public static boolean isOneLakeHost(String host) {
    return host != null && ONELAKE_HOST.matcher(host).matches();
  }

  static boolean isGuid(String value) {
    return value != null && GUID.matcher(value).matches();
  }

  /**
   * Returns the {@code fabric-onelake} dataset name ({@code ws.item.schema.table}) for a OneLake
   * table path, or empty when the path is not a (resolvable) OneLake table path or the mapping is
   * disabled.
   */
  public static Optional<String> toDatasetName(URI uri, DatahubOpenlineageConfig config) {
    if (uri == null || config == null || !config.isFabricOneLakeEnabled()) {
      return Optional.empty();
    }
    String scheme = uri.getScheme();
    if (scheme == null || !SCHEMES.contains(scheme.toLowerCase(Locale.ROOT))) {
      return Optional.empty();
    }
    if (!isOneLakeHost(uri.getHost())) {
      return Optional.empty();
    }
    String workspace = uri.getUserInfo();
    String path = uri.getPath();
    if (workspace == null || workspace.isEmpty() || path == null) {
      return Optional.empty();
    }

    List<String> segments =
        Arrays.stream(path.split("/")).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    // {item}/Tables/{table} at minimum
    if (segments.size() < 3 || !TABLES_FOLDER.equalsIgnoreCase(segments.get(1))) {
      return Optional.empty();
    }

    Optional<String[]> ids = resolveWorkspaceAndItem(workspace, segments.get(0), config);
    if (!ids.isPresent()) {
      return Optional.empty();
    }

    List<String> tableSegments = stripNonTableSegments(segments.subList(2, segments.size()));
    String schema;
    String table;
    if (tableSegments.size() == 1) {
      schema = DEFAULT_SCHEMA;
      table = tableSegments.get(0);
    } else if (tableSegments.size() == 2) {
      schema = tableSegments.get(0);
      table = tableSegments.get(1);
    } else {
      log.debug("OneLake path {} is not a Tables/[schema/]table path; not mapping it", uri);
      return Optional.empty();
    }
    if (config.isFabricOneLakeConvertUrnsToLowercase()) {
      schema = schema.toLowerCase(Locale.ROOT);
      table = table.toLowerCase(Locale.ROOT);
    }
    return Optional.of(String.join(".", ids.get()[0], ids.get()[1], schema, table));
  }

  /**
   * Drops trailing segments below the table root: Delta internals ({@code _delta_log}, other {@code
   * _}-prefixed folders) and Hive-style partition folders ({@code key=value}).
   */
  private static List<String> stripNonTableSegments(List<String> segments) {
    List<String> result = new ArrayList<>();
    for (String segment : segments) {
      if (segment.startsWith("_") || segment.contains("=")) {
        break;
      }
      result.add(segment);
    }
    return result;
  }

  private static Optional<String[]> resolveWorkspaceAndItem(
      String workspace, String item, DatahubOpenlineageConfig config) {
    // GUID-form item references need no item type, but tolerate a ".ItemType" suffix.
    String itemId = item.contains(".") ? item.substring(0, item.indexOf('.')) : item;
    if (isGuid(workspace) && isGuid(itemId)) {
      // Fabric REST APIs (and therefore the fabric-onelake connector) return lowercase GUIDs.
      return Optional.of(
          new String[] {workspace.toLowerCase(Locale.ROOT), itemId.toLowerCase(Locale.ROOT)});
    }
    Map<String, String> itemIds = config.getFabricOneLakeItemIds();
    if (itemIds == null || itemIds.isEmpty()) {
      log.debug(
          "OneLake path uses friendly names ({}/{}) and no fabricOneLake itemIds mapping is"
              + " configured; leaving it on the abs platform",
          workspace,
          item);
      return Optional.empty();
    }
    String key = workspace + "/" + item;
    for (Map.Entry<String, String> entry : itemIds.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(key)) {
        String[] parts = entry.getValue().split("/");
        if (parts.length == 2 && isGuid(parts[0].trim()) && isGuid(parts[1].trim())) {
          return Optional.of(
              new String[] {
                parts[0].trim().toLowerCase(Locale.ROOT), parts[1].trim().toLowerCase(Locale.ROOT)
              });
        }
        log.warn(
            "Invalid fabricOneLake itemIds value '{}' for '{}'; expected"
                + " <workspaceGUID>/<itemGUID>",
            entry.getValue(),
            entry.getKey());
        return Optional.empty();
      }
    }
    log.debug("No fabricOneLake itemIds mapping for '{}'; leaving it on the abs platform", key);
    return Optional.empty();
  }

  /**
   * Parses a comma-separated list of {@code <workspaceName>/<itemName>.<ItemType>=<workspaceGUID>/
   * <itemGUID>} entries into a map. Malformed entries are skipped with a warning.
   */
  public static Map<String, String> parseItemIds(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> result = new HashMap<>();
    for (String entry : value.split(",")) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int eq = trimmed.indexOf('=');
      if (eq <= 0 || eq == trimmed.length() - 1) {
        log.warn(
            "Ignoring malformed fabricOneLake itemIds entry '{}'; expected"
                + " <workspaceName>/<itemName>.<ItemType>=<workspaceGUID>/<itemGUID>",
            trimmed);
        continue;
      }
      result.put(trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim());
    }
    return result;
  }
}
