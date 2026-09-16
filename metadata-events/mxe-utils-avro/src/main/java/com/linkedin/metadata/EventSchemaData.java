package com.linkedin.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.avro.Schema;

/**
 * EventSchemaData provides access to schema mappings and configuration. This class can be
 * instantiated and loads configuration from YAML or falls back to hardcoded mappings.
 */
@Getter
public class EventSchemaData {
  private final Map<String, List<Integer>> schemaNameToIdsMap;
  private final Map<Integer, String> schemaIdToNameMap;
  private final Map<Integer, List<Integer>> schemaIdToVersionsMap;
  private final Map<Integer, Map<Integer, Schema>> schemaIdToVersionedSchemasMap;
  private final Map<String, String> schemaCompatibilityMap;
  private final Map<String, Map<Integer, Integer>> schemaRegistryIdMap;

  public EventSchemaData() {
    this(new ObjectMapper());
  }

  public EventSchemaData(ObjectMapper objectMapper) {
    try {
      SchemaConfigLoader configLoader = new SchemaConfigLoader(objectMapper);
      SchemaConfigLoader.SchemaConfig config = configLoader.getConfig();

      // Generate mappings from loaded configuration
      Map<String, List<Integer>> schemaNameToIdsMap = generateSchemaNameToSchemaIdsMap(config);
      Map<Integer, String> schemaIdToNameMap = generateSchemaIdToSchemaNameMap(config);
      Map<Integer, List<Integer>> schemaIdToVersionsMap = generateSchemaIdToVersionsMap(config);
      Map<Integer, Map<Integer, Schema>> schemaIdToVersionedSchemasMap =
          generateSchemaIdToVersionedSchemasMap(config);
      Map<String, String> schemaCompatibilityMap = generateSchemaCompatibilityMap(config);
      Map<String, Map<Integer, Integer>> schemaRegistryIdMap = generateSchemaRegistryIdMap(config);

      // Assign to final fields
      this.schemaNameToIdsMap = Collections.unmodifiableMap(schemaNameToIdsMap);
      this.schemaIdToNameMap = Collections.unmodifiableMap(schemaIdToNameMap);
      this.schemaIdToVersionsMap = Collections.unmodifiableMap(schemaIdToVersionsMap);
      this.schemaIdToVersionedSchemasMap =
          Collections.unmodifiableMap(schemaIdToVersionedSchemasMap);
      this.schemaCompatibilityMap = Collections.unmodifiableMap(schemaCompatibilityMap);
      this.schemaRegistryIdMap = Collections.unmodifiableMap(schemaRegistryIdMap);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize EventSchemaData", e);
    }
  }

  public List<Integer> getSchemaIdsForSchemaName(String schemaName) {
    return schemaNameToIdsMap.getOrDefault(schemaName, Collections.emptyList());
  }

  public String getSchemaNameForSchemaId(int schemaId) {
    return schemaIdToNameMap.get(schemaId);
  }

  public List<Integer> getVersionsForSchemaId(int schemaId) {
    return schemaIdToVersionsMap.getOrDefault(schemaId, Collections.emptyList());
  }

  public Map<Integer, Schema> getVersionedSchemasForSchemaId(int schemaId) {
    return schemaIdToVersionedSchemasMap.get(schemaId);
  }

  public String getCompatibilityForSchemaName(String schemaName) {
    return schemaCompatibilityMap.getOrDefault(schemaName, "NONE");
  }

  public Schema getSchemaForSchemaIdAndVersion(int schemaId, int version) {
    Map<Integer, Schema> versionedSchemas = schemaIdToVersionedSchemasMap.get(schemaId);
    if (versionedSchemas != null) {
      return versionedSchemas.get(version);
    }
    return null;
  }

  private Map<String, List<Integer>> generateSchemaNameToSchemaIdsMap(
      SchemaConfigLoader.SchemaConfig config) {
    Map<String, List<Integer>> result = new HashMap<>();

    // Process each schema definition from YAML
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry :
        config.getSchemas().entrySet()) {
      String schemaName = entry.getKey();
      SchemaConfigLoader.SchemaDefinition schemaDef = entry.getValue();

      // Derive schema_ids from versions
      Set<Integer> schemaIdSet = collectUniqueSchemaIdsFromVersions(schemaDef.getVersions());
      result.put(schemaName, new ArrayList<>(schemaIdSet));
    }

    return result;
  }

  private Map<Integer, String> generateSchemaIdToSchemaNameMap(
      SchemaConfigLoader.SchemaConfig config) {
    Map<Integer, String> result = new HashMap<>();

    // Process each schema definition from YAML
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry :
        config.getSchemas().entrySet()) {
      String yamlSchemaName = entry.getKey();
      SchemaConfigLoader.SchemaDefinition schemaDef = entry.getValue();

      // Map each schema ID from versions to the schema name
      for (SchemaConfigLoader.VersionDefinition versionDef : schemaDef.getVersions()) {
        try {
          int schemaId = SchemaIdOrdinal.valueOf(versionDef.getOrdinalId()).getSchemaId();
          result.put(schemaId, yamlSchemaName);
        } catch (IllegalArgumentException e) {
          throw new RuntimeException(
              "Invalid ordinal_id '"
                  + versionDef.getOrdinalId()
                  + "' in schema '"
                  + yamlSchemaName
                  + "' version "
                  + versionDef.getVersion()
                  + ". "
                  + "Valid ordinals are: "
                  + Arrays.toString(SchemaIdOrdinal.values()),
              e);
        }
      }
    }

    return result;
  }

  private Map<Integer, List<Integer>> generateSchemaIdToVersionsMap(
      SchemaConfigLoader.SchemaConfig config) {
    Map<Integer, List<Integer>> result = new HashMap<>();

    // Process each schema definition from YAML
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry :
        config.getSchemas().entrySet()) {
      SchemaConfigLoader.SchemaDefinition schemaDef = entry.getValue();

      // Process each version definition
      for (SchemaConfigLoader.VersionDefinition versionDef : schemaDef.getVersions()) {
        try {
          int ordinalId = SchemaIdOrdinal.valueOf(versionDef.getOrdinalId()).getSchemaId();
          result.computeIfAbsent(ordinalId, k -> new ArrayList<>()).add(versionDef.getVersion());
        } catch (IllegalArgumentException e) {
          throw new RuntimeException(
              "Invalid ordinal_id '"
                  + versionDef.getOrdinalId()
                  + "' in schema definition. "
                  + "Valid ordinals are: "
                  + Arrays.toString(SchemaIdOrdinal.values()),
              e);
        }
      }
    }

    return result;
  }

  /**
   * Generate schema registry IDs for each version of each schema. Each version uses its ordinal ID
   * as the schema registry ID.
   */
  private Map<String, Map<Integer, Integer>> generateSchemaRegistryIdMap(
      SchemaConfigLoader.SchemaConfig config) {
    Map<String, Map<Integer, Integer>> result = new HashMap<>();

    // Process each schema definition from YAML
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry :
        config.getSchemas().entrySet()) {
      String schemaName = entry.getKey();
      SchemaConfigLoader.SchemaDefinition schemaDef = entry.getValue();

      Map<Integer, Integer> versionToSchemaRegistryId = new HashMap<>();

      // Use the ordinal ID as the schema registry ID for each version
      for (SchemaConfigLoader.VersionDefinition versionDef : schemaDef.getVersions()) {
        try {
          int ordinalId = SchemaIdOrdinal.valueOf(versionDef.getOrdinalId()).getSchemaId();
          versionToSchemaRegistryId.put(versionDef.getVersion(), ordinalId);
        } catch (IllegalArgumentException e) {
          throw new RuntimeException(
              "Invalid ordinal_id '"
                  + versionDef.getOrdinalId()
                  + "' in schema '"
                  + schemaName
                  + "' version "
                  + versionDef.getVersion()
                  + ". "
                  + "Valid ordinals are: "
                  + Arrays.toString(SchemaIdOrdinal.values()),
              e);
        }
      }

      result.put(schemaName, versionToSchemaRegistryId);
    }

    return result;
  }

  private Map<Integer, Map<Integer, Schema>> generateSchemaIdToVersionedSchemasMap(
      SchemaConfigLoader.SchemaConfig config) {
    Map<Integer, Map<Integer, Schema>> result = new HashMap<>();

    // Process each schema definition from YAML
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry :
        config.getSchemas().entrySet()) {
      String yamlSchemaName = entry.getKey();
      SchemaConfigLoader.SchemaDefinition schemaDef = entry.getValue();

      // Build versioned schemas map for this schema definition
      Map<Integer, Schema> versionedSchemas = buildVersionedSchemasMap(schemaDef.getVersions());

      // Map each unique schema ID from versions to the versioned schemas
      Set<Integer> uniqueSchemaIds = collectUniqueSchemaIdsFromVersions(schemaDef.getVersions());
      for (Integer schemaId : uniqueSchemaIds) {
        result.put(schemaId, versionedSchemas);
      }
    }

    return result;
  }

  private Map<String, String> generateSchemaCompatibilityMap(
      SchemaConfigLoader.SchemaConfig config) {
    Map<String, String> compatibilityMap = new HashMap<>();

    // Process each schema definition from YAML
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry :
        config.getSchemas().entrySet()) {
      String yamlSchemaName = entry.getKey();
      SchemaConfigLoader.SchemaDefinition schemaDef = entry.getValue();

      // Get the corresponding EventUtils constant name
      compatibilityMap.put(yamlSchemaName, schemaDef.getCompatibility());
    }

    return compatibilityMap;
  }

  /**
   * Helper method to collect unique schema IDs from a list of version definitions. This reduces
   * code duplication across multiple generation methods.
   */
  private Set<Integer> collectUniqueSchemaIdsFromVersions(
      List<SchemaConfigLoader.VersionDefinition> versions) {
    Set<Integer> schemaIdSet = new HashSet<>();
    for (SchemaConfigLoader.VersionDefinition versionDef : versions) {
      try {
        int ordinalId = SchemaIdOrdinal.valueOf(versionDef.getOrdinalId()).getSchemaId();
        schemaIdSet.add(ordinalId);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(
            "Invalid ordinal_id '"
                + versionDef.getOrdinalId()
                + "' in version "
                + versionDef.getVersion()
                + ". "
                + "Valid ordinals are: "
                + Arrays.toString(SchemaIdOrdinal.values()),
            e);
      }
    }
    return schemaIdSet;
  }

  /**
   * Helper method to build a map of version numbers to Schema objects from version definitions.
   * This extracts the complex logic for mapping versions to actual Schema objects.
   */
  private Map<Integer, Schema> buildVersionedSchemasMap(
      List<SchemaConfigLoader.VersionDefinition> versions) {
    Map<Integer, Schema> versionedSchemas = new HashMap<>();

    for (SchemaConfigLoader.VersionDefinition versionDef : versions) {
      try {
        SchemaIdOrdinal ordinal = SchemaIdOrdinal.valueOf(versionDef.getOrdinalId());
        Schema schema = EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.get(ordinal);
        if (schema != null) {
          versionedSchemas.put(versionDef.getVersion(), schema);
        } else {
          throw new RuntimeException(
              "Schema mapping not found for ordinal '"
                  + versionDef.getOrdinalId()
                  + "' in version "
                  + versionDef.getVersion()
                  + ". "
                  + "This ordinal is defined in SchemaIdOrdinal but not mapped in EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.");
        }
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(
            "Invalid ordinal_id '"
                + versionDef.getOrdinalId()
                + "' in version "
                + versionDef.getVersion()
                + ". "
                + "Valid ordinals are: "
                + Arrays.toString(SchemaIdOrdinal.values()),
            e);
      }
    }

    return versionedSchemas;
  }

  /**
   * Get the unique schema registry ID for a specific schema name and version. Each version gets its
   * own unique schema registry ID (e.g., 1001, 1247, 1589, 2104).
   *
   * @param schemaName the schema name (e.g., "MetadataChangeLog")
   * @param version the version number (e.g., 1, 2, 3, 4, 5, 6)
   * @return the unique schema registry ID for this version, or null if not found
   */
  public Integer getSchemaRegistryId(String schemaName, int version) {
    Map<Integer, Integer> versionToIdMap = schemaRegistryIdMap.get(schemaName);
    if (versionToIdMap != null) {
      return versionToIdMap.get(version);
    }
    return null;
  }
}
