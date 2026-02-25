package com.linkedin.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

  // Map from SchemaIdOrdinal enum to actual Schema objects
  public static final Map<SchemaIdOrdinal, Schema> SCHEMA_ID_TO_SCHEMA_MAP;

  static {
    Map<SchemaIdOrdinal, Schema> map = new HashMap<>();
    map.put(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1, MCP_V1_SCHEMA);
    map.put(
        SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1_FIX,
        MCP_V1_SCHEMA); // Same as V1 for backward compatibility
    map.put(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL, MCP_SCHEMA);
    map.put(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1, FMCP_V1_SCHEMA);
    map.put(
        SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1_FIX,
        FMCP_V1_SCHEMA); // Same as V1 for backward compatibility
    map.put(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL, FMCP_SCHEMA);
    map.put(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1, MCL_V1_SCHEMA);
    map.put(
        SchemaIdOrdinal.METADATA_CHANGE_LOG_V1_FIX,
        MCL_V1_SCHEMA); // Same as V1 for backward compatibility
    map.put(SchemaIdOrdinal.METADATA_CHANGE_LOG, MCL_SCHEMA);
    map.put(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1, MCL_TIMESERIES_V1_SCHEMA);
    map.put(
        SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1_FIX,
        MCL_TIMESERIES_V1_SCHEMA); // Same as V1 for backward compatibility
    map.put(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES, MCL_TIMESERIES_SCHEMA);
    map.put(SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1, MCE_V1_SCHEMA);
    map.put(
        SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1_FIX,
        MCE_V1_SCHEMA); // Same as V1 for backward compatibility
    map.put(SchemaIdOrdinal.METADATA_CHANGE_EVENT, MCE_SCHEMA);
    map.put(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1, FMCE_V1_SCHEMA);
    map.put(
        SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1_FIX,
        FMCE_V1_SCHEMA); // Same as V1 for backward compatibility
    map.put(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT, FMCE_SCHEMA);
    map.put(SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1, MAE_V1_SCHEMA);
    map.put(
        SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1_FIX,
        MAE_V1_SCHEMA); // Same as V1 for backward compatibility
    map.put(SchemaIdOrdinal.METADATA_AUDIT_EVENT, MAE_SCHEMA);
    map.put(SchemaIdOrdinal.PLATFORM_EVENT, PE_SCHEMA);
    map.put(SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT, DUHE_SCHEMA);

    // Validation: Ensure all SchemaIdOrdinal enum values are mapped to schemas
    validateAllOrdinalsMapped(map);

    SCHEMA_ID_TO_SCHEMA_MAP = Map.copyOf(map);
  }

  // Schema compatibility constants
  /**
   * NONE: No compatibility checking is done. This means that any schema changes are allowed. This
   * is the most permissive mode.
   */
  public static final String SCHEMA_COMPATIBILITY_NONE = "NONE";

  /**
   * BACKWARD: New schema must be backward compatible with existing data. This means that existing
   * data can be read with the new schema, but new data written with the new schema may not be
   * readable with the old schema.
   */
  public static final String SCHEMA_COMPATIBILITY_BACKWARD = "BACKWARD";

  /**
   * FORWARD: New schema must be forward compatible with existing consumers. This means that new
   * data written with the new schema can be read with the old schema, but existing data may not be
   * readable with the new schema.
   */
  public static final String SCHEMA_COMPATIBILITY_FORWARD = "FORWARD";

  /**
   * FULL: New schema must be both backward and forward compatible. This is the most restrictive
   * mode and ensures that both old and new data can be read with either schema version.
   */
  public static final String SCHEMA_COMPATIBILITY_FULL = "FULL";

  /**
   * Validate that all SchemaIdOrdinal enum values are mapped to schemas. This ensures we don't miss
   * any ordinals when adding new schemas.
   *
   * @param map the schema mapping to validate
   * @throws RuntimeException if any ordinal is missing from the map
   */
  private static void validateAllOrdinalsMapped(Map<SchemaIdOrdinal, Schema> map) {
    Set<SchemaIdOrdinal> missingOrdinals = new HashSet<>();

    for (SchemaIdOrdinal ordinal : SchemaIdOrdinal.values()) {
      if (!map.containsKey(ordinal)) {
        missingOrdinals.add(ordinal);
      }
    }

    if (!missingOrdinals.isEmpty()) {
      throw new RuntimeException(
          "Missing schema mappings for ordinals: "
              + missingOrdinals
              + ". All SchemaIdOrdinal enum values must be mapped to schemas in SCHEMA_ID_TO_SCHEMA_MAP.");
    }
  }

  /**
   * Load a schema from a resource file.
   *
   * @param resourceName the name of the resource file
   * @return the loaded schema
   * @throws RuntimeException if the schema cannot be loaded
   */
  private static Schema loadSchemaFromResource(String resourceName) {
    try (InputStream inputStream =
        EventSchemaConstants.class.getClassLoader().getResourceAsStream(resourceName)) {
      if (inputStream == null) {
        throw new RuntimeException("Schema resource not found: " + resourceName);
      }
      return new Schema.Parser().parse(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Error loading schema from resource: " + resourceName, e);
    }
  }
}
