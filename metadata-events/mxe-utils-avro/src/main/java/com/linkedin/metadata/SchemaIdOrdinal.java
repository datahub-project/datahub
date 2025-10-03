package com.linkedin.metadata;

/**
 * Schema ID ordinals for the Schema Registry Service.
 *
 * <p>IMPORTANT: These ordinal values must NOT be changed as they are used by external systems and
 * breaking changes would cause serialization/deserialization failures.
 *
 * <p>The ordinals are preserved from the original TopicOrdinal enum to maintain backward
 * compatibility.
 */
public enum SchemaIdOrdinal {
  METADATA_CHANGE_PROPOSAL_V1(0),
  FAILED_METADATA_CHANGE_PROPOSAL_V1(1),
  METADATA_CHANGE_LOG_V1(2),
  METADATA_CHANGE_LOG_TIMESERIES_V1(3),
  PLATFORM_EVENT(4),
  METADATA_CHANGE_EVENT_V1(5),
  FAILED_METADATA_CHANGE_EVENT_V1(6),
  METADATA_AUDIT_EVENT_V1(7),
  DATAHUB_UPGRADE_HISTORY_EVENT(8),
  METADATA_CHANGE_PROPOSAL_V1_FIX(9),
  FAILED_METADATA_CHANGE_PROPOSAL_V1_FIX(10),
  METADATA_CHANGE_LOG_V1_FIX(11),
  METADATA_CHANGE_LOG_TIMESERIES_V1_FIX(12),
  METADATA_CHANGE_EVENT_V1_FIX(13),
  FAILED_METADATA_CHANGE_EVENT_V1_FIX(14),
  METADATA_AUDIT_EVENT_V1_FIX(15),

  // Breaking changes from V1
  METADATA_CHANGE_PROPOSAL(16),
  FAILED_METADATA_CHANGE_PROPOSAL(17),
  METADATA_CHANGE_LOG(18),
  METADATA_CHANGE_LOG_TIMESERIES(19),
  METADATA_CHANGE_EVENT(20),
  FAILED_METADATA_CHANGE_EVENT(21),
  METADATA_AUDIT_EVENT(22);

  private final int schemaId;

  SchemaIdOrdinal(int schemaId) {
    this.schemaId = schemaId;
  }

  /**
   * Get the schema ID for this ordinal
   *
   * @return the schema ID
   */
  public int getSchemaId() {
    return schemaId;
  }

  /**
   * Get the SchemaIdOrdinal by schema ID
   *
   * @param schemaId the schema ID to look up
   * @return the SchemaIdOrdinal, or null if not found
   */
  public static SchemaIdOrdinal fromSchemaId(int schemaId) {
    for (SchemaIdOrdinal ordinal : values()) {
      if (ordinal.schemaId == schemaId) {
        return ordinal;
      }
    }
    return null;
  }

  /**
   * Check if a schema ID is valid
   *
   * @param schemaId the schema ID to validate
   * @return true if the schema ID is valid, false otherwise
   */
  public static boolean isValidSchemaId(int schemaId) {
    return fromSchemaId(schemaId) != null;
  }

  /**
   * Get all valid schema IDs
   *
   * @return array of all valid schema IDs
   */
  public static int[] getAllSchemaIds() {
    SchemaIdOrdinal[] ordinals = values();
    int[] schemaIds = new int[ordinals.length];
    for (int i = 0; i < ordinals.length; i++) {
      schemaIds[i] = ordinals[i].schemaId;
    }
    return schemaIds;
  }
}
