package io.datahubproject.openapi.openlineage.config;

import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.FabricOneLakePath;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Microsoft Fabric options for one OpenLineage request, passed as query parameters or headers.
 *
 * <p>The producer decides how its own events are mapped, without GMS configuration or a restart:
 *
 * <pre>
 * POST /openapi/openlineage/api/v1/lineage?fabricOneLake=true&amp;fabricOneLakeConvertUrnsToLowercase=true
 * X-DataHub-Fabric-OneLake: true
 * </pre>
 *
 * A query parameter wins over the header of the same option. With neither set, the endpoint's
 * mapping is unchanged (OneLake paths stay on {@code abs} / catalog-symlink URNs), so existing
 * producers keep their URNs.
 */
public final class FabricRequestOptions {

  /** One option: its query parameter and its header. */
  enum Option {
    FABRIC_ONELAKE("fabricOneLake", "X-DataHub-Fabric-OneLake"),
    CONVERT_URNS_TO_LOWERCASE(
        "fabricOneLakeConvertUrnsToLowercase",
        "X-DataHub-Fabric-OneLake-Convert-Urns-To-Lowercase"),
    PLATFORM_INSTANCE(
        "fabricOneLakePlatformInstance", "X-DataHub-Fabric-OneLake-Platform-Instance"),
    ITEM_IDS("fabricOneLakeItemIds", "X-DataHub-Fabric-OneLake-Item-Ids"),
    NOTEBOOK_FLOW_NAMES("fabricNotebookFlowNames", "X-DataHub-Fabric-Notebook-Flow-Names");

    final String parameter;
    final String header;

    Option(String parameter, String header) {
      this.parameter = parameter;
      this.header = header;
    }
  }

  private FabricRequestOptions() {}

  /**
   * The endpoint's converter config with this request's Fabric options applied.
   *
   * @throws IllegalArgumentException for a malformed option; the endpoint answers 400
   */
  public static DatahubOpenlineageConfig apply(
      DatahubOpenlineageConfig base, HttpServletRequest request) {
    String oneLake = value(request, Option.FABRIC_ONELAKE);
    String lowercase = value(request, Option.CONVERT_URNS_TO_LOWERCASE);
    String platformInstance = value(request, Option.PLATFORM_INSTANCE);
    String itemIds = value(request, Option.ITEM_IDS);
    String notebookFlowNames = value(request, Option.NOTEBOOK_FLOW_NAMES);
    if (oneLake == null
        && lowercase == null
        && platformInstance == null
        && itemIds == null
        && notebookFlowNames == null) {
      return base;
    }

    boolean oneLakeEnabled = bool(Option.FABRIC_ONELAKE, oneLake);
    if (!oneLakeEnabled && (lowercase != null || platformInstance != null || itemIds != null)) {
      throw new IllegalArgumentException(
          Option.CONVERT_URNS_TO_LOWERCASE.parameter
              + ", "
              + Option.PLATFORM_INSTANCE.parameter
              + " and "
              + Option.ITEM_IDS.parameter
              + " apply to the OneLake mapping; set "
              + Option.FABRIC_ONELAKE.parameter
              + "=true too");
    }
    DatahubOpenlineageConfig.DatahubOpenlineageConfigBuilder builder = base.toBuilder();
    if (oneLake != null) {
      builder.fabricOneLakeEnabled(oneLakeEnabled);
    }
    if (lowercase != null) {
      builder.fabricOneLakeConvertUrnsToLowercase(
          bool(Option.CONVERT_URNS_TO_LOWERCASE, lowercase));
    }
    if (platformInstance != null) {
      builder.fabricOneLakePlatformInstance(platformInstance);
    }
    if (itemIds != null) {
      builder.fabricOneLakeItemIds(itemIds(itemIds));
    }
    if (notebookFlowNames != null) {
      builder.fabricNotebookFlowNames(bool(Option.NOTEBOOK_FLOW_NAMES, notebookFlowNames));
    }
    return builder.build();
  }

  private static String value(HttpServletRequest request, Option option) {
    String value = request.getParameter(option.parameter);
    if (value == null) {
      value = request.getHeader(option.header);
    }
    if (value == null) {
      return null;
    }
    value = value.trim();
    if (value.isEmpty()) {
      throw new IllegalArgumentException(option.parameter + " is empty");
    }
    return value;
  }

  private static boolean bool(Option option, String value) {
    if (value == null) {
      return false;
    }
    switch (value.toLowerCase(Locale.ROOT)) {
      case "true":
        return true;
      case "false":
        return false;
      default:
        throw new IllegalArgumentException(
            option.parameter + " must be true or false, got '" + value + "'");
    }
  }

  /** Every entry must parse: a request can't silently lose a mapping the producer asked for. */
  private static Map<String, String> itemIds(String value) {
    Map<String, String> parsed = FabricOneLakePath.parseItemIds(value);
    long entries = Arrays.stream(value.split(",")).filter(e -> !e.isBlank()).count();
    if (parsed.size() != entries) {
      throw new IllegalArgumentException(
          Option.ITEM_IDS.parameter
              + " has malformed or duplicate entries; expected comma-separated"
              + " <workspaceName>/<itemName>.<ItemType>=<workspaceGUID>/<itemGUID>");
    }
    return new HashMap<>(parsed);
  }
}
