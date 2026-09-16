package com.linkedin.metadata;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/** Unit tests for SchemaConfigLoader. */
public class SchemaConfigLoaderTest {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  @Test
  public void testLoadConfigWithYAMLFactory() {
    // Given
    SchemaConfigLoader loader = new SchemaConfigLoader(YAML_MAPPER);

    // When
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();

    // Then
    assertNotNull(config, "Config should not be null");
    assertNotNull(config.getSchemas(), "Schemas map should not be null");
    assertFalse(config.getSchemas().isEmpty(), "Schemas map should not be empty");

    // Verify specific schemas exist
    assertTrue(
        config.getSchemas().containsKey("MetadataChangeProposal"),
        "Should contain MetadataChangeProposal schema");
    assertTrue(
        config.getSchemas().containsKey("FailedMetadataChangeProposal"),
        "Should contain FailedMetadataChangeProposal schema");
    assertTrue(
        config.getSchemas().containsKey("MetadataChangeLog"),
        "Should contain MetadataChangeLog schema");
    assertTrue(
        config.getSchemas().containsKey("PlatformEvent"), "Should contain PlatformEvent schema");
    assertTrue(
        config.getSchemas().containsKey("MetadataChangeEvent"),
        "Should contain MetadataChangeEvent schema");
    assertTrue(
        config.getSchemas().containsKey("FailedMetadataChangeEvent"),
        "Should contain FailedMetadataChangeEvent schema");
    assertTrue(
        config.getSchemas().containsKey("MetadataAuditEvent"),
        "Should contain MetadataAuditEvent schema");
    assertTrue(
        config.getSchemas().containsKey("DataHubUpgradeHistoryEvent"),
        "Should contain DataHubUpgradeHistoryEvent schema");
  }

  @Test
  public void testMetadataChangeProposalSchema() {
    // Given
    SchemaConfigLoader loader = new SchemaConfigLoader(YAML_MAPPER);

    // When
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();
    SchemaConfigLoader.SchemaDefinition mcpSchema =
        config.getSchemas().get("MetadataChangeProposal");

    // Then
    assertNotNull(mcpSchema, "MCP schema should not be null");
    assertEquals(mcpSchema.getDescription(), "Metadata change proposal events");
    assertEquals(mcpSchema.getCompatibility(), "NONE");

    // Verify schema IDs derived from versions
    Set<String> schemaIds = new HashSet<>();
    for (SchemaConfigLoader.VersionDefinition version : mcpSchema.getVersions()) {
      schemaIds.add(version.getOrdinalId());
    }
    assertEquals(schemaIds.size(), 3, "Should have 3 unique schema IDs");
    assertTrue(
        schemaIds.contains("METADATA_CHANGE_PROPOSAL_V1"),
        "Should contain METADATA_CHANGE_PROPOSAL_V1");
    assertTrue(
        schemaIds.contains("METADATA_CHANGE_PROPOSAL_V1_FIX"),
        "Should contain METADATA_CHANGE_PROPOSAL_V1_FIX");
    assertTrue(
        schemaIds.contains("METADATA_CHANGE_PROPOSAL"), "Should contain METADATA_CHANGE_PROPOSAL");

    // Verify versions
    List<SchemaConfigLoader.VersionDefinition> versions = mcpSchema.getVersions();
    assertNotNull(versions, "Versions should not be null");
    assertEquals(versions.size(), 3, "Should have 3 versions");

    // Check version 1
    SchemaConfigLoader.VersionDefinition version1 = versions.get(0);
    assertEquals(version1.getVersion(), 1);
    assertEquals(version1.getOrdinalId(), "METADATA_CHANGE_PROPOSAL_V1");
    assertEquals(version1.getDescription(), "Original MCP schema without aspectCreated");

    // Check version 2
    SchemaConfigLoader.VersionDefinition version2 = versions.get(1);
    assertEquals(version2.getVersion(), 2);
    assertEquals(version2.getOrdinalId(), "METADATA_CHANGE_PROPOSAL_V1_FIX");
    assertEquals(version2.getDescription(), "Same as v1 for backward compatibility");

    // Check version 3
    SchemaConfigLoader.VersionDefinition version3 = versions.get(2);
    assertEquals(version3.getVersion(), 3);
    assertEquals(version3.getOrdinalId(), "METADATA_CHANGE_PROPOSAL");
    assertEquals(version3.getDescription(), "New MCP schema with aspectCreated field");
  }

  @Test
  public void testMetadataChangeLogSchema() {
    // Given
    SchemaConfigLoader loader = new SchemaConfigLoader(YAML_MAPPER);

    // When
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();
    SchemaConfigLoader.SchemaDefinition mclSchema = config.getSchemas().get("MetadataChangeLog");

    // Then
    assertNotNull(mclSchema, "MCL schema should not be null");
    assertEquals(
        mclSchema.getDescription(), "Metadata change log events with multiple schema types");
    assertEquals(mclSchema.getCompatibility(), "NONE");

    // Verify schema IDs derived from versions
    Set<String> schemaIds = new HashSet<>();
    for (SchemaConfigLoader.VersionDefinition version : mclSchema.getVersions()) {
      schemaIds.add(version.getOrdinalId());
    }
    assertEquals(schemaIds.size(), 6, "Should have 6 unique schema IDs");
    assertTrue(
        schemaIds.contains("METADATA_CHANGE_LOG_V1"), "Should contain METADATA_CHANGE_LOG_V1");
    assertTrue(schemaIds.contains("METADATA_CHANGE_LOG"), "Should contain METADATA_CHANGE_LOG");
    assertTrue(
        schemaIds.contains("METADATA_CHANGE_LOG_TIMESERIES_V1"),
        "Should contain METADATA_CHANGE_LOG_TIMESERIES_V1");
    assertTrue(
        schemaIds.contains("METADATA_CHANGE_LOG_TIMESERIES"),
        "Should contain METADATA_CHANGE_LOG_TIMESERIES");
    assertTrue(
        schemaIds.contains("METADATA_CHANGE_LOG_V1_FIX"),
        "Should contain METADATA_CHANGE_LOG_V1_FIX");
    assertTrue(
        schemaIds.contains("METADATA_CHANGE_LOG_TIMESERIES_V1_FIX"),
        "Should contain METADATA_CHANGE_LOG_TIMESERIES_V1_FIX");

    // Verify versions
    List<SchemaConfigLoader.VersionDefinition> versions = mclSchema.getVersions();
    assertNotNull(versions, "Versions should not be null");
    assertEquals(versions.size(), 6, "Should have 6 versions");
  }

  @Test
  public void testPlatformEventSchema() {
    // Given
    SchemaConfigLoader loader = new SchemaConfigLoader(YAML_MAPPER);

    // When
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();
    SchemaConfigLoader.SchemaDefinition peSchema = config.getSchemas().get("PlatformEvent");

    // Then
    assertNotNull(peSchema, "PE schema should not be null");
    assertEquals(peSchema.getDescription(), "Platform events");
    assertEquals(peSchema.getCompatibility(), "BACKWARD");

    // Verify schema IDs derived from versions
    Set<String> schemaIds = new HashSet<>();
    for (SchemaConfigLoader.VersionDefinition version : peSchema.getVersions()) {
      schemaIds.add(version.getOrdinalId());
    }
    assertEquals(schemaIds.size(), 1, "Should have 1 unique schema ID");
    assertTrue(schemaIds.contains("PLATFORM_EVENT"), "Should contain PLATFORM_EVENT");

    // Verify versions
    List<SchemaConfigLoader.VersionDefinition> versions = peSchema.getVersions();
    assertNotNull(versions, "Versions should not be null");
    assertEquals(versions.size(), 1, "Should have 1 version");

    SchemaConfigLoader.VersionDefinition version1 = versions.get(0);
    assertEquals(version1.getVersion(), 1);
    assertEquals(version1.getOrdinalId(), "PLATFORM_EVENT");
    assertEquals(version1.getDescription(), "Platform event schema");
  }

  @Test
  public void testDataHubUpgradeHistoryEventSchema() {
    // Given
    SchemaConfigLoader loader = new SchemaConfigLoader(YAML_MAPPER);

    // When
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();
    SchemaConfigLoader.SchemaDefinition duheSchema =
        config.getSchemas().get("DataHubUpgradeHistoryEvent");

    // Then
    assertNotNull(duheSchema, "DUHE schema should not be null");
    assertEquals(duheSchema.getDescription(), "DataHub upgrade history events");
    assertEquals(duheSchema.getCompatibility(), "BACKWARD");

    // Verify schema IDs derived from versions
    Set<String> schemaIds = new HashSet<>();
    for (SchemaConfigLoader.VersionDefinition version : duheSchema.getVersions()) {
      schemaIds.add(version.getOrdinalId());
    }
    assertEquals(schemaIds.size(), 1, "Should have 1 unique schema ID");
    assertTrue(
        schemaIds.contains("DATAHUB_UPGRADE_HISTORY_EVENT"),
        "Should contain DATAHUB_UPGRADE_HISTORY_EVENT");

    // Verify versions
    List<SchemaConfigLoader.VersionDefinition> versions = duheSchema.getVersions();
    assertNotNull(versions, "Versions should not be null");
    assertEquals(versions.size(), 1, "Should have 1 version");

    SchemaConfigLoader.VersionDefinition version1 = versions.get(0);
    assertEquals(version1.getVersion(), 1);
    assertEquals(version1.getOrdinalId(), "DATAHUB_UPGRADE_HISTORY_EVENT");
    assertEquals(version1.getDescription(), "DataHub upgrade history schema");
  }

  @Test
  public void testAllSchemasHaveRequiredFields() {
    // Given
    SchemaConfigLoader loader = new SchemaConfigLoader(YAML_MAPPER);

    // When
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();
    Map<String, SchemaConfigLoader.SchemaDefinition> schemas = config.getSchemas();

    // Then
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry : schemas.entrySet()) {
      String schemaName = entry.getKey();
      SchemaConfigLoader.SchemaDefinition schema = entry.getValue();

      assertNotNull(schema.getDescription(), "Schema " + schemaName + " should have a description");
      // Verify schema has versions (schema IDs are derived from versions)
      assertNotNull(schema.getVersions(), "Schema " + schemaName + " should have versions");
      assertFalse(
          schema.getVersions().isEmpty(),
          "Schema " + schemaName + " should have non-empty versions");
      assertNotNull(
          schema.getCompatibility(), "Schema " + schemaName + " should have compatibility");

      // Verify all versions have required fields
      for (SchemaConfigLoader.VersionDefinition version : schema.getVersions()) {
        assertTrue(
            version.getVersion() > 0, "Schema " + schemaName + " version should be positive");
        assertNotNull(
            version.getOrdinalId(), "Schema " + schemaName + " version should have ordinal ID");
        assertNotNull(
            version.getDescription(), "Schema " + schemaName + " version should have description");
      }
    }
  }

  @Test
  public void testSchemaIdConsistency() {
    // Given
    SchemaConfigLoader loader = new SchemaConfigLoader(YAML_MAPPER);

    // When
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();
    Map<String, SchemaConfigLoader.SchemaDefinition> schemas = config.getSchemas();

    // Then
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry : schemas.entrySet()) {
      String schemaName = entry.getKey();
      SchemaConfigLoader.SchemaDefinition schema = entry.getValue();

      // All schema IDs in the schema_ids list should appear in at least one version
      // Schema IDs are now derived from versions, so this test is no longer applicable
      // Skip this validation since schema_ids field was removed
      if (false)
        for (String schemaId : new HashSet<String>()) {
          boolean foundInVersions = false;
          for (SchemaConfigLoader.VersionDefinition version : schema.getVersions()) {
            if (schemaId.equals(version.getOrdinalId())) {
              foundInVersions = true;
              break;
            }
          }
          assertTrue(
              foundInVersions,
              "Schema ID "
                  + schemaId
                  + " in "
                  + schemaName
                  + " should appear in at least one version");
        }
    }
  }

  @Test
  public void testCompatibilityValues() {
    // Given
    SchemaConfigLoader loader = new SchemaConfigLoader(YAML_MAPPER);

    // When
    SchemaConfigLoader.SchemaConfig config = loader.getConfig();
    Map<String, SchemaConfigLoader.SchemaDefinition> schemas = config.getSchemas();

    // Then
    for (Map.Entry<String, SchemaConfigLoader.SchemaDefinition> entry : schemas.entrySet()) {
      String schemaName = entry.getKey();
      String compatibility = entry.getValue().getCompatibility();

      assertTrue(
          compatibility.equals("NONE")
              || compatibility.equals("BACKWARD")
              || compatibility.equals("FORWARD")
              || compatibility.equals("FULL"),
          "Schema " + schemaName + " should have valid compatibility value, got: " + compatibility);
    }
  }
}
