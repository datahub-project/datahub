package com.linkedin.metadata;

import static org.testng.Assert.*;

import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

public class EventSchemaConstantsTest {

  @Test
  public void testGetSchemaIdsForSchemaName() {
    // Test that we can get schema IDs for known schema names
    var mcpSchemaIds =
        EventSchemaConstants.getSchemaIdsForSchemaName(
            EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    assertTrue(mcpSchemaIds.isPresent());
    assertEquals(mcpSchemaIds.get().size(), 2);
    assertTrue(mcpSchemaIds.get().contains(EventSchemaConstants.MCP_V1_SCHEMA_ID));
    assertTrue(mcpSchemaIds.get().contains(EventSchemaConstants.MCP_SCHEMA_ID));

    var mclSchemaIds =
        EventSchemaConstants.getSchemaIdsForSchemaName(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    assertTrue(mclSchemaIds.isPresent());
    assertEquals(mclSchemaIds.get().size(), 4);
    assertTrue(mclSchemaIds.get().contains(EventSchemaConstants.MCL_V1_SCHEMA_ID));
    assertTrue(mclSchemaIds.get().contains(EventSchemaConstants.MCL_SCHEMA_ID));
    assertTrue(mclSchemaIds.get().contains(EventSchemaConstants.MCL_TIMESERIES_V1_SCHEMA_ID));
    assertTrue(mclSchemaIds.get().contains(EventSchemaConstants.MCL_TIMESERIES_SCHEMA_ID));

    var singleVersionSchemaIds =
        EventSchemaConstants.getSchemaIdsForSchemaName(
            EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME);
    assertTrue(singleVersionSchemaIds.isPresent());
    assertEquals(singleVersionSchemaIds.get().size(), 2);
    assertTrue(singleVersionSchemaIds.get().contains(EventSchemaConstants.MCE_V1_SCHEMA_ID));
    assertTrue(singleVersionSchemaIds.get().contains(EventSchemaConstants.MCE_SCHEMA_ID));
  }

  @Test
  public void testGetSchemaNameForSchemaId() {
    // Test that we can get schema names for known schema IDs
    var mcpSchemaName =
        EventSchemaConstants.getSchemaNameForSchemaId(EventSchemaConstants.MCP_SCHEMA_ID);
    assertTrue(mcpSchemaName.isPresent());
    assertEquals(mcpSchemaName.get(), EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);

    var mclSchemaName =
        EventSchemaConstants.getSchemaNameForSchemaId(EventSchemaConstants.MCL_SCHEMA_ID);
    assertTrue(mclSchemaName.isPresent());
    assertEquals(mclSchemaName.get(), EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);

    var duheSchemaName =
        EventSchemaConstants.getSchemaNameForSchemaId(EventSchemaConstants.DUHE_SCHEMA_ID);
    assertTrue(duheSchemaName.isPresent());
    assertEquals(duheSchemaName.get(), EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);
  }

  @Test
  public void testGetSchemaNameForSchemaIdWithUnknownId() {
    // Test that unknown schema IDs return empty
    var unknownSchemaName = EventSchemaConstants.getSchemaNameForSchemaId(999);
    assertFalse(unknownSchemaName.isPresent());
  }

  @Test
  public void testGetSchemaIdsForSchemaNameWithUnknownName() {
    // Test that unknown schema names return empty
    var unknownSchemaIds = EventSchemaConstants.getSchemaIdsForSchemaName("UnknownSchema");
    assertFalse(unknownSchemaIds.isPresent());
  }

  @Test
  public void testGetSchemaIdsForSchemaNameWithNull() {
    // Test that null schema names return empty
    var nullSchemaIds = EventSchemaConstants.getSchemaIdsForSchemaName(null);
    assertFalse(nullSchemaIds.isPresent());
  }

  @Test
  public void testGetAllSchemaNames() {
    // Test that we can get all schema names
    List<String> allSchemaNames = EventSchemaConstants.getAllSchemaNames();
    assertNotNull(allSchemaNames);
    assertTrue(allSchemaNames.size() > 0);

    // Verify that known schema names are included
    assertTrue(allSchemaNames.contains(EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME));
    assertTrue(allSchemaNames.contains(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME));
    assertTrue(allSchemaNames.contains(EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME));
    assertTrue(allSchemaNames.contains(EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME));
  }

  @Test
  public void testSchemaIdOrdinalMapping() {
    // Test that schema IDs match their ordinal values
    assertEquals(EventSchemaConstants.MCP_V1_SCHEMA_ID, 0);
    assertEquals(EventSchemaConstants.FMCP_V1_SCHEMA_ID, 1);
    assertEquals(EventSchemaConstants.MCL_V1_SCHEMA_ID, 2);
    assertEquals(EventSchemaConstants.MCL_TIMESERIES_V1_SCHEMA_ID, 3);
    assertEquals(EventSchemaConstants.PE_SCHEMA_ID, 4);
    assertEquals(EventSchemaConstants.MCE_V1_SCHEMA_ID, 5);
    assertEquals(EventSchemaConstants.FMCE_V1_SCHEMA_ID, 6);
    assertEquals(EventSchemaConstants.MAE_V1_SCHEMA_ID, 7);
    assertEquals(EventSchemaConstants.DUHE_SCHEMA_ID, 8);
    assertEquals(EventSchemaConstants.MCP_SCHEMA_ID, 9);
    assertEquals(EventSchemaConstants.FMCP_SCHEMA_ID, 10);
    assertEquals(EventSchemaConstants.MCL_SCHEMA_ID, 11);
    assertEquals(EventSchemaConstants.MCL_TIMESERIES_SCHEMA_ID, 12);
    assertEquals(EventSchemaConstants.MCE_SCHEMA_ID, 13);
    assertEquals(EventSchemaConstants.FMCE_SCHEMA_ID, 14);
    assertEquals(EventSchemaConstants.MAE_SCHEMA_ID, 15);
  }

  @Test
  public void testSchemaCompatibilityConstants() {
    // Test that schema compatibility constants are defined and have expected values
    assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, "NONE");
    assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD, "BACKWARD");
    assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_FORWARD, "FORWARD");
    assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_FULL, "FULL");

    // Test that constants are not null
    assertNotNull(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE);
    assertNotNull(EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD);
    assertNotNull(EventSchemaConstants.SCHEMA_COMPATIBILITY_FORWARD);
    assertNotNull(EventSchemaConstants.SCHEMA_COMPATIBILITY_FULL);
  }

  @Test
  public void testSchemaConstantsNotNull() {
    // Test that all schema constants are not null
    assertNotNull(EventSchemaConstants.MCP_V1_SCHEMA);
    assertNotNull(EventSchemaConstants.MCL_V1_SCHEMA);
    assertNotNull(EventSchemaConstants.MCL_TIMESERIES_V1_SCHEMA);
    assertNotNull(EventSchemaConstants.MCE_V1_SCHEMA);
    assertNotNull(EventSchemaConstants.FMCE_V1_SCHEMA);
    assertNotNull(EventSchemaConstants.MAE_V1_SCHEMA);
    assertNotNull(EventSchemaConstants.MCP_SCHEMA);
    assertNotNull(EventSchemaConstants.MCL_SCHEMA);
    assertNotNull(EventSchemaConstants.MCL_TIMESERIES_SCHEMA);
    assertNotNull(EventSchemaConstants.PE_SCHEMA);
    assertNotNull(EventSchemaConstants.MCE_SCHEMA);
    assertNotNull(EventSchemaConstants.FMCE_SCHEMA);
    assertNotNull(EventSchemaConstants.MAE_SCHEMA);
    assertNotNull(EventSchemaConstants.DUHE_SCHEMA);
    assertNotNull(EventSchemaConstants.FMCP_V1_SCHEMA);
    assertNotNull(EventSchemaConstants.FMCP_SCHEMA);
  }

  @Test
  public void testSchemaConstantsAreValidSchemas() {
    // Test that all schema constants are valid Avro schemas
    assertTrue(EventSchemaConstants.MCP_V1_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MCL_V1_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MCL_TIMESERIES_V1_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MCE_V1_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.FMCE_V1_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MAE_V1_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MCP_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MCL_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MCL_TIMESERIES_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.PE_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MCE_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.FMCE_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.MAE_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.DUHE_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.FMCP_V1_SCHEMA instanceof Schema);
    assertTrue(EventSchemaConstants.FMCP_SCHEMA instanceof Schema);
  }

  @Test
  public void testGetSchemaNameToSchemaIdsMap() {
    // Test that we can get the schema name to schema IDs mapping
    Map<String, List<Integer>> schemaMap = EventSchemaConstants.getSchemaNameToSchemaIdsMap();
    assertNotNull(schemaMap);
    assertFalse(schemaMap.isEmpty());

    // Test that the map is unmodifiable
    try {
      schemaMap.put("Test", List.of(999));
      fail("Map should be unmodifiable");
    } catch (UnsupportedOperationException e) {
      // Expected
    }

    // Verify specific mappings
    assertTrue(schemaMap.containsKey(EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME));
    assertTrue(schemaMap.containsKey(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME));
    assertTrue(schemaMap.containsKey(EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME));
    assertTrue(schemaMap.containsKey(EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME));
  }

  @Test
  public void testGetSchemaIdToSchemaNameMap() {
    // Test that we can get the schema ID to schema name mapping
    Map<Integer, String> schemaMap = EventSchemaConstants.getSchemaIdToSchemaNameMap();
    assertNotNull(schemaMap);
    assertFalse(schemaMap.isEmpty());

    // Test that the map is unmodifiable
    try {
      schemaMap.put(999, "Test");
      fail("Map should be unmodifiable");
    } catch (UnsupportedOperationException e) {
      // Expected
    }

    // Verify specific mappings
    assertTrue(schemaMap.containsKey(EventSchemaConstants.MCP_V1_SCHEMA_ID));
    assertTrue(schemaMap.containsKey(EventSchemaConstants.MCP_SCHEMA_ID));
    assertTrue(schemaMap.containsKey(EventSchemaConstants.DUHE_SCHEMA_ID));
    assertTrue(schemaMap.containsKey(EventSchemaConstants.PE_SCHEMA_ID));
  }

  @Test
  public void testSchemaNameToSchemaIdsMappingConsistency() {
    // Test that schema name to schema IDs mapping is consistent
    Map<String, List<Integer>> nameToIdsMap = EventSchemaConstants.getSchemaNameToSchemaIdsMap();
    Map<Integer, String> idToNameMap = EventSchemaConstants.getSchemaIdToSchemaNameMap();

    // For each schema name, verify that all its schema IDs map back to the same name
    for (Map.Entry<String, List<Integer>> entry : nameToIdsMap.entrySet()) {
      String schemaName = entry.getKey();
      List<Integer> schemaIds = entry.getValue();

      for (Integer schemaId : schemaIds) {
        String mappedName = idToNameMap.get(schemaId);
        assertEquals(
            mappedName,
            schemaName,
            "Schema ID "
                + schemaId
                + " should map to "
                + schemaName
                + " but maps to "
                + mappedName);
      }
    }
  }

  @Test
  public void testSchemaIdToSchemaNameMappingConsistency() {
    // Test that schema ID to schema name mapping is consistent
    Map<String, List<Integer>> nameToIdsMap = EventSchemaConstants.getSchemaNameToSchemaIdsMap();
    Map<Integer, String> idToNameMap = EventSchemaConstants.getSchemaIdToSchemaNameMap();

    // For each schema ID, verify that its schema name contains the ID in its list
    for (Map.Entry<Integer, String> entry : idToNameMap.entrySet()) {
      Integer schemaId = entry.getKey();
      String schemaName = entry.getValue();

      List<Integer> schemaIds = nameToIdsMap.get(schemaName);
      assertNotNull(schemaIds, "Schema name " + schemaName + " should have a list of schema IDs");
      assertTrue(
          schemaIds.contains(schemaId),
          "Schema name " + schemaName + " should contain schema ID " + schemaId);
    }
  }

  @Test
  public void testAllSchemaNamesAreMapped() {
    // Test that all schema names have corresponding schema IDs
    List<String> allSchemaNames = EventSchemaConstants.getAllSchemaNames();
    Map<String, List<Integer>> nameToIdsMap = EventSchemaConstants.getSchemaNameToSchemaIdsMap();

    for (String schemaName : allSchemaNames) {
      assertTrue(
          nameToIdsMap.containsKey(schemaName),
          "Schema name " + schemaName + " should have a mapping to schema IDs");
      List<Integer> schemaIds = nameToIdsMap.get(schemaName);
      assertNotNull(schemaIds);
      assertFalse(
          schemaIds.isEmpty(), "Schema name " + schemaName + " should have at least one schema ID");
    }
  }

  @Test
  public void testAllSchemaIdsAreMapped() {
    // Test that all schema IDs have corresponding schema names
    Map<Integer, String> idToNameMap = EventSchemaConstants.getSchemaIdToSchemaNameMap();
    Map<String, List<Integer>> nameToIdsMap = EventSchemaConstants.getSchemaNameToSchemaIdsMap();

    // Collect all schema IDs from the name to IDs mapping
    for (List<Integer> schemaIds : nameToIdsMap.values()) {
      for (Integer schemaId : schemaIds) {
        assertTrue(
            idToNameMap.containsKey(schemaId),
            "Schema ID " + schemaId + " should have a mapping to a schema name");
        String schemaName = idToNameMap.get(schemaId);
        assertNotNull(schemaName);
        assertFalse(schemaName.isEmpty());
      }
    }
  }

  @Test
  public void testSchemaConstantsHaveExpectedNamespaces() {
    // Test that V1 schemas have the expected pegasus2avro namespace
    assertTrue(
        EventSchemaConstants.MCP_V1_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.MCL_V1_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.MCE_V1_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.FMCE_V1_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.MAE_V1_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.FMCP_V1_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));

    // Test that current schemas have the expected pegasus2avro namespace
    assertTrue(
        EventSchemaConstants.MCP_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.MCL_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.MCE_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.FMCE_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.MAE_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.FMCP_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(
        EventSchemaConstants.DUHE_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
    assertTrue(EventSchemaConstants.PE_SCHEMA.getNamespace().contains("linkedin.pegasus2avro.mxe"));
  }
}
