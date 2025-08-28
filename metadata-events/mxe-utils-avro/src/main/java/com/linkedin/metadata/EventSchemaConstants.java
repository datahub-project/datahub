package com.linkedin.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Arrays;
import org.apache.avro.Schema;

/** Constants and utility methods for avro schema */
public final class EventSchemaConstants {

  private EventSchemaConstants() {
    // Utility class, prevent instantiation
  }

  // Legacy v1 schema loaded from resource for backward compatibility
  public static final Schema MCP_V1_SCHEMA =
      loadSchemaFromResource(
          "v1/avro/com/linkedin/mxe/" + EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME + ".avsc");
  public static final Schema MCL_V1_SCHEMA =
      loadSchemaFromResource(
          "v1/avro/com/linkedin/mxe/" + EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME + ".avsc");
  public static final Schema MCL_TIMESERIES_V1_SCHEMA =
      loadSchemaFromResource(
          "v1/avro/com/linkedin/mxe/" + EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME + ".avsc");
  public static final Schema MCE_V1_SCHEMA =
      loadSchemaFromResource(
          "v1/avro/com/linkedin/mxe/" + EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME + ".avsc");
  public static final Schema FMCE_V1_SCHEMA =
      loadSchemaFromResource(
          "v1/avro/com/linkedin/mxe/"
              + EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME
              + ".avsc");
  public static final Schema MAE_V1_SCHEMA =
      loadSchemaFromResource(
          "v1/avro/com/linkedin/mxe/" + EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME + ".avsc");

  // Current schemas used elsewhere in the codebase - use schemas with correct namespace
  public static final Schema MCP_SCHEMA = EventUtils.RENAMED_MCP_AVRO_SCHEMA;
  public static final Schema MCL_SCHEMA = EventUtils.RENAMED_MCL_AVRO_SCHEMA;
  public static final Schema MCL_TIMESERIES_SCHEMA = EventUtils.RENAMED_MCL_AVRO_SCHEMA;
  public static final Schema PE_SCHEMA = EventUtils.RENAMED_PE_AVRO_SCHEMA;
  public static final Schema MCE_SCHEMA = EventUtils.RENAMED_MCE_AVRO_SCHEMA;
  public static final Schema FMCE_SCHEMA = EventUtils.RENAMED_FAILED_MCE_AVRO_SCHEMA;
  public static final Schema MAE_SCHEMA = EventUtils.RENAMED_MAE_AVRO_SCHEMA;
  public static final Schema DUHE_SCHEMA = EventUtils.RENAMED_DUHE_AVRO_SCHEMA;
  public static final Schema FMCP_V1_SCHEMA =
      loadSchemaFromResource(
          "v1/avro/com/linkedin/mxe/"
              + EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME
              + ".avsc");
  public static final Schema FMCP_SCHEMA = EventUtils.RENAMED_FMCP_AVRO_SCHEMA;

  // Schema ID mappings for proper schema resolution
  // V1 schemas (backward compatible) - these get their own schema IDs
  public static final int MCP_V1_SCHEMA_ID =
      SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1.getSchemaId();
  public static final int FMCP_V1_SCHEMA_ID =
      SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1.getSchemaId();
  public static final int MCL_V1_SCHEMA_ID = SchemaIdOrdinal.METADATA_CHANGE_LOG_V1.getSchemaId();
  public static final int MCL_TIMESERIES_V1_SCHEMA_ID =
      SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1.getSchemaId();
  public static final int MCE_V1_SCHEMA_ID = SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1.getSchemaId();
  public static final int FMCE_V1_SCHEMA_ID =
      SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1.getSchemaId();
  public static final int MAE_V1_SCHEMA_ID = SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1.getSchemaId();

  // Current schemas (incompatible with V1) - these get new schema IDs
  public static final int MCP_SCHEMA_ID = SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL.getSchemaId();
  public static final int FMCP_SCHEMA_ID =
      SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL.getSchemaId();
  public static final int MCL_SCHEMA_ID = SchemaIdOrdinal.METADATA_CHANGE_LOG.getSchemaId();
  public static final int MCL_TIMESERIES_SCHEMA_ID =
      SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES.getSchemaId();
  public static final int MCE_SCHEMA_ID = SchemaIdOrdinal.METADATA_CHANGE_EVENT.getSchemaId();
  public static final int FMCE_SCHEMA_ID =
      SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT.getSchemaId();
  public static final int MAE_SCHEMA_ID = SchemaIdOrdinal.METADATA_AUDIT_EVENT.getSchemaId();
  public static final int DUHE_SCHEMA_ID =
      SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT.getSchemaId();

  // Single version schemas (backward compatible)
  public static final int PE_SCHEMA_ID = SchemaIdOrdinal.PLATFORM_EVENT.getSchemaId();

  // Schema name to list of schema IDs mapping
  private static final Map<String, List<Integer>> SCHEMA_NAME_TO_SCHEMA_IDS_MAP = new HashMap<>();

  // Schema ID to schema name mapping
  private static final Map<Integer, String> SCHEMA_ID_TO_SCHEMA_NAME_MAP = new HashMap<>();

  static {
    // Map schema names to their list of schema IDs
    // Each schema ID represents a different version of the schema (backwards incompatible)
    SCHEMA_NAME_TO_SCHEMA_IDS_MAP.put(
        EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME, Arrays.asList(MCP_V1_SCHEMA_ID, MCP_SCHEMA_ID));
    SCHEMA_NAME_TO_SCHEMA_IDS_MAP.put(
        EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME,
        Arrays.asList(FMCP_V1_SCHEMA_ID, FMCP_SCHEMA_ID));
    SCHEMA_NAME_TO_SCHEMA_IDS_MAP.put(
        EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME,
        Arrays.asList(
            MCL_V1_SCHEMA_ID,
            MCL_SCHEMA_ID,
            MCL_TIMESERIES_V1_SCHEMA_ID,
            MCL_TIMESERIES_SCHEMA_ID));
    SCHEMA_NAME_TO_SCHEMA_IDS_MAP.put(EventUtils.PLATFORM_EVENT_SCHEMA_NAME, Arrays.asList(PE_SCHEMA_ID));
    SCHEMA_NAME_TO_SCHEMA_IDS_MAP.put(
        EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME, Arrays.asList(MCE_V1_SCHEMA_ID, MCE_SCHEMA_ID));
    SCHEMA_NAME_TO_SCHEMA_IDS_MAP.put(
        EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME,
        Arrays.asList(FMCE_V1_SCHEMA_ID, FMCE_SCHEMA_ID));
    SCHEMA_NAME_TO_SCHEMA_IDS_MAP.put(
        EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME, Arrays.asList(MAE_V1_SCHEMA_ID, MAE_SCHEMA_ID));
    SCHEMA_NAME_TO_SCHEMA_IDS_MAP.put(
        EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME, Arrays.asList(DUHE_SCHEMA_ID));

    // Map schema IDs to their corresponding schema names
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        MCP_V1_SCHEMA_ID, EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        MCP_SCHEMA_ID, EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        FMCP_V1_SCHEMA_ID, EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        FMCP_SCHEMA_ID, EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(MCL_V1_SCHEMA_ID, EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(MCL_SCHEMA_ID, EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        MCL_TIMESERIES_V1_SCHEMA_ID, EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        MCL_TIMESERIES_SCHEMA_ID, EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(PE_SCHEMA_ID, EventUtils.PLATFORM_EVENT_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        MCE_V1_SCHEMA_ID, EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(MCE_SCHEMA_ID, EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        FMCE_V1_SCHEMA_ID, EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        FMCE_SCHEMA_ID, EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(MAE_V1_SCHEMA_ID, EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(MAE_SCHEMA_ID, EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME);
    SCHEMA_ID_TO_SCHEMA_NAME_MAP.put(
        DUHE_SCHEMA_ID, EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);
  }

  public static Map<String, List<Integer>> getSchemaNameToSchemaIdsMap() {
    return Collections.unmodifiableMap(SCHEMA_NAME_TO_SCHEMA_IDS_MAP);
  }

  public static Map<Integer, String> getSchemaIdToSchemaNameMap() {
    return Collections.unmodifiableMap(SCHEMA_ID_TO_SCHEMA_NAME_MAP);
  }

  public static Optional<List<Integer>> getSchemaIdsForSchemaName(String schemaName) {
    return Optional.ofNullable(SCHEMA_NAME_TO_SCHEMA_IDS_MAP.get(schemaName));
  }

  public static Optional<String> getSchemaNameForSchemaId(int schemaId) {
    return Optional.ofNullable(SCHEMA_ID_TO_SCHEMA_NAME_MAP.get(schemaId));
  }

  public static List<String> getAllSchemaNames() {
    return new ArrayList<>(SCHEMA_NAME_TO_SCHEMA_IDS_MAP.keySet());
  }

  // Schema compatibility constants
  /**
   * NONE: No compatibility checking is done. This means that any schema changes are allowed. This
   * is the most permissive mode and should be used when you want to make breaking changes without
   * any compatibility guarantees.
   */
  public static final String SCHEMA_COMPATIBILITY_NONE = "NONE";

  /**
   * BACKWARD: New schema can read data written by the previous schema. This means that new
   * consumers can read old data, but old consumers cannot read new data. This is useful when you
   * want to add new fields or make fields optional.
   */
  public static final String SCHEMA_COMPATIBILITY_BACKWARD = "BACKWARD";

  /**
   * FORWARD: Previous schema can read data written by the new schema. This means that old consumers
   * can read new data, but new consumers cannot read old data. This is useful when you want to
   * remove fields or make fields required.
   */
  public static final String SCHEMA_COMPATIBILITY_FORWARD = "FORWARD";

  /**
   * FULL: Both backward and forward compatibility are maintained. This means that both old and new
   * consumers can read data written by either schema. This is the most restrictive mode and
   * provides the strongest compatibility guarantees.
   */
  public static final String SCHEMA_COMPATIBILITY_FULL = "FULL";

  private static Schema loadSchemaFromResource(String resourceName) {
    try (InputStream inputStream =
        EventSchemaConstants.class.getClassLoader().getResourceAsStream(resourceName)) {
      if (inputStream == null) {
        throw new RuntimeException("Could not find schema resource: " + resourceName);
      }
      return new Schema.Parser().parse(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Error loading schema from resource: " + resourceName, e);
    }
  }
}
