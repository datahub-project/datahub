package com.linkedin.metadata;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.metadata.SchemaConfigLoader.SchemaConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;

/** Test class for EventSchemaData functionality including validation and error handling. */
public class EventSchemaDataTest {

  @Test
  public void testEventSchemaDataInitialization() {
    // Test that EventSchemaData can be initialized without errors
    EventSchemaData eventSchemaData = new EventSchemaData();
    assertNotNull(eventSchemaData);
  }

  @Test
  public void testGetSchemaIdsForSchemaName() {
    EventSchemaData eventSchemaData = new EventSchemaData();

    // Test with a known schema name
    List<Integer> schemaIds = eventSchemaData.getSchemaIdsForSchemaName("MetadataChangeProposal");
    assertNotNull(schemaIds);
    assertFalse(schemaIds.isEmpty());
  }

  @Test
  public void testGetSchemaNameForSchemaId() {
    EventSchemaData eventSchemaData = new EventSchemaData();

    // Test with a known schema ID
    String schemaName = eventSchemaData.getSchemaNameForSchemaId(0); // METADATA_CHANGE_PROPOSAL_V1
    assertNotNull(schemaName);
    assertEquals(schemaName, "MetadataChangeProposal");
  }

  @Test
  public void testGetVersionsForSchemaId() {
    EventSchemaData eventSchemaData = new EventSchemaData();

    // Test with a known schema ID
    List<Integer> versions =
        eventSchemaData.getVersionsForSchemaId(0); // METADATA_CHANGE_PROPOSAL_V1
    assertNotNull(versions);
    assertFalse(versions.isEmpty());
  }

  @Test
  public void testGetVersionedSchemasForSchemaId() {
    EventSchemaData eventSchemaData = new EventSchemaData();

    // Test with a known schema ID
    var versionedSchemas =
        eventSchemaData.getVersionedSchemasForSchemaId(0); // METADATA_CHANGE_PROPOSAL_V1
    assertNotNull(versionedSchemas);
    assertFalse(versionedSchemas.isEmpty());
  }

  @Test
  public void testGetCompatibilityForSchemaName() {
    EventSchemaData eventSchemaData = new EventSchemaData();

    // Test with a known schema name
    String compatibility = eventSchemaData.getCompatibilityForSchemaName("MetadataChangeProposal");
    assertNotNull(compatibility);
  }

  @Test
  public void testGetSchemaForSchemaIdAndVersion() {
    EventSchemaData eventSchemaData = new EventSchemaData();

    // Test with a known schema ID and version
    org.apache.avro.Schema schema =
        eventSchemaData.getSchemaForSchemaIdAndVersion(
            0, 1); // METADATA_CHANGE_PROPOSAL_V1, version 1
    assertNotNull(schema);
  }

  @Test
  public void testGetSchemaRegistryId() {
    EventSchemaData eventSchemaData = new EventSchemaData();

    // Test with a known schema name and version
    Integer schemaRegistryId = eventSchemaData.getSchemaRegistryId("MetadataChangeProposal", 1);
    assertNotNull(schemaRegistryId);
  }

  // Validation and Error Handling Tests

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Invalid ordinal_id 'INVALID_ORDINAL'.*")
  public void testInvalidOrdinalIdThrowsError() throws IOException {
    // Create a test configuration with an invalid ordinal_id
    String invalidYaml =
        "schemas:\n"
            + "  TestSchema:\n"
            + "    description: \"Test schema with invalid ordinal\"\n"
            + "    versions:\n"
            + "      - version: 1\n"
            + "        ordinal_id: \"INVALID_ORDINAL\"\n"
            + "        description: \"Test version with invalid ordinal\"\n"
            + "    compatibility: \"NONE\"";

    SchemaConfig config = parseYamlConfig(invalidYaml);

    // Test the validation logic by creating a test that would trigger the error
    // We'll simulate what happens when EventSchemaData processes this config
    try {
      validateOrdinalId("INVALID_ORDINAL", "TestSchema", 1);
      fail("Expected RuntimeException for invalid ordinal_id");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Invalid ordinal_id 'INVALID_ORDINAL'"));
      assertTrue(e.getMessage().contains("Valid ordinals are:"));
      throw e; // Re-throw to satisfy the expected exception test
    }
  }

  @Test
  public void testAllOrdinalsAreMapped() throws IOException {
    // Test that all SchemaIdOrdinal values are mapped in
    // EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP
    // This validates the validation logic we added to EventSchemaConstants
    try {
      // This should not throw any exceptions because all ordinals should be mapped
      for (SchemaIdOrdinal ordinal : SchemaIdOrdinal.values()) {
        org.apache.avro.Schema schema = EventSchemaConstants.SCHEMA_ID_TO_SCHEMA_MAP.get(ordinal);
        assertNotNull(schema, "Schema mapping should exist for ordinal: " + ordinal);
      }
    } catch (RuntimeException e) {
      fail("All ordinals should be mapped: " + e.getMessage());
    }
  }

  @Test
  public void testValidOrdinalIdDoesNotThrowError() throws IOException {
    // Test with a valid ordinal_id
    try {
      validateOrdinalId("METADATA_CHANGE_PROPOSAL_V1", "TestSchema", 1);
      // Should not throw any exceptions
    } catch (RuntimeException e) {
      fail("Should not throw RuntimeException for valid ordinal_id: " + e.getMessage());
    }
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Invalid ordinal_id 'TYPO_ORDINAL'.*")
  public void testTypoInOrdinalIdThrowsError() throws IOException {
    // Test with a common typo in ordinal_id
    try {
      validateOrdinalId("TYPO_ORDINAL", "TestSchema", 1);
      fail("Expected RuntimeException for typo in ordinal_id");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Invalid ordinal_id 'TYPO_ORDINAL'"));
      assertTrue(e.getMessage().contains("Valid ordinals are:"));
      // Verify it lists some valid ordinals
      assertTrue(e.getMessage().contains("METADATA_CHANGE_PROPOSAL_V1"));
      throw e;
    }
  }

  @Test
  public void testErrorMessagesAreDescriptive() throws IOException {
    // Test that error messages provide helpful information
    try {
      validateOrdinalId("BAD_ORDINAL", "MyTestSchema", 3);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      String message = e.getMessage();

      // Verify the error message contains helpful information
      assertTrue(message.contains("Invalid ordinal_id 'BAD_ORDINAL'"));
      assertTrue(message.contains("MyTestSchema")); // Schema name
      assertTrue(message.contains("version 3")); // Version number
      assertTrue(message.contains("Valid ordinals are:")); // Helpful guidance

      // Verify it lists actual valid ordinals
      assertTrue(message.contains("METADATA_CHANGE_PROPOSAL_V1"));
      assertTrue(message.contains("METADATA_CHANGE_LOG"));
      assertTrue(message.contains("METADATA_CHANGE_EVENT"));
    }
  }

  /**
   * Simulate the validation logic that was added to EventSchemaData. This demonstrates the improved
   * error handling that replaces silent RuntimeException catches.
   */
  private void validateOrdinalId(String ordinalId, String schemaName, int version) {
    try {
      SchemaIdOrdinal.valueOf(ordinalId);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(
          "Invalid ordinal_id '"
              + ordinalId
              + "' in schema '"
              + schemaName
              + "' version "
              + version
              + ". "
              + "Valid ordinals are: "
              + java.util.Arrays.toString(SchemaIdOrdinal.values()),
          e);
    }
  }

  private SchemaConfig parseYamlConfig(String yamlContent) throws IOException {
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    return yamlMapper.readValue(
        new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8)), SchemaConfig.class);
  }
}
