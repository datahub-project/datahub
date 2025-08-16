package com.linkedin.metadata;

import com.linkedin.pegasus2avro.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeProposal;
import com.linkedin.pegasus2avro.mxe.MetadataAuditEvent;
import com.linkedin.pegasus2avro.mxe.MetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.MetadataChangeLog;
import com.linkedin.pegasus2avro.mxe.MetadataChangeProposal;
import com.linkedin.pegasus2avro.mxe.PlatformEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;

/** Constants and utility methods for avro schema */
public final class EventSchemaConstants {

  private EventSchemaConstants() {
    // Utility class, prevent instantiation
  }

  // Schema version constants
  public static final int SCHEMA_VERSION_1 = 1;
  public static final int SCHEMA_VERSION_2 = 2;

  // Schema-level version mapping - stores all supported versions for each schema
  private static final Map<String, List<Integer>> SCHEMA_VERSIONS = new HashMap<>();

  static {
    // MetadataChangeProposal schema supports multiple versions
    SCHEMA_VERSIONS.put(
        EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME,
        List.of(SCHEMA_VERSION_1, SCHEMA_VERSION_2));

    // FailedMetadataChangeProposal schema supports multiple versions
    SCHEMA_VERSIONS.put(
        EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME,
        List.of(SCHEMA_VERSION_1, SCHEMA_VERSION_2));

    // Other schemas only support version 1
    SCHEMA_VERSIONS.put(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME, List.of(SCHEMA_VERSION_1));
    SCHEMA_VERSIONS.put(EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME, List.of(SCHEMA_VERSION_1));
    SCHEMA_VERSIONS.put(
        EventUtils.FAILED_METADATA_CHANGE_EVENT_SCHEMA_NAME, List.of(SCHEMA_VERSION_1));
    SCHEMA_VERSIONS.put(EventUtils.METADATA_AUDIT_EVENT_SCHEMA_NAME, List.of(SCHEMA_VERSION_1));
    SCHEMA_VERSIONS.put(EventUtils.PLATFORM_EVENT_SCHEMA_NAME, List.of(SCHEMA_VERSION_1));
    SCHEMA_VERSIONS.put(
        EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME, List.of(SCHEMA_VERSION_1));
  }

  /** Get the latest schema version for a specific schema type */
  public static int getLatestSchemaVersion(String schemaName) {
    List<Integer> versions = SCHEMA_VERSIONS.get(schemaName);
    if (versions != null && !versions.isEmpty()) {
      return versions.stream().mapToInt(Integer::intValue).max().orElse(SCHEMA_VERSION_1);
    }
    return SCHEMA_VERSION_1;
  }

  /** Get all supported versions for a specific schema type */
  public static List<Integer> getSupportedVersions(String schemaName) {
    return SCHEMA_VERSIONS.getOrDefault(schemaName, List.of(SCHEMA_VERSION_1));
  }

  // Legacy v1 schema loaded from resource for backward compatibility
  public static final Schema MCP_V1_SCHEMA =
      loadSchemaFromResource(EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME + "_v1.avsc");
  // Current schemas used elsewhere in the codebase
  public static final Schema MCP_SCHEMA = MetadataChangeProposal.getClassSchema();
  public static final Schema MCL_SCHEMA = MetadataChangeLog.getClassSchema();
  public static final Schema MCL_TIMESERIES_SCHEMA = MetadataChangeLog.getClassSchema();
  public static final Schema PE_SCHEMA = PlatformEvent.getClassSchema();
  public static final Schema MCE_SCHEMA = MetadataChangeEvent.getClassSchema();
  public static final Schema FMCE_SCHEMA = FailedMetadataChangeEvent.getClassSchema();
  public static final Schema MAE_SCHEMA = MetadataAuditEvent.getClassSchema();
  public static final Schema DUHE_SCHEMA = DataHubUpgradeHistoryEvent.getClassSchema();
  public static final Schema FMCP_V1_SCHEMA =
      loadSchemaFromResource(EventUtils.FAILED_METADATA_CHANGE_PROPOSAL_SCHEMA_NAME + "_v1.avsc");
  public static final Schema FMCP_SCHEMA = FailedMetadataChangeProposal.getClassSchema();

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
