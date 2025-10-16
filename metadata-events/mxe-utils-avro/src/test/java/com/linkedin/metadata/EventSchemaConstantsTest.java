package com.linkedin.metadata;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

public class EventSchemaConstantsTest {

  @Test
  public void testGetSchemaIdsForSchemaName() {
    // Test that we can get schema IDs for known schema names
    EventSchemaData schemaData = new EventSchemaData();

    var mcpSchemaIds =
        schemaData.getSchemaIdsForSchemaName(EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    assertFalse(mcpSchemaIds.isEmpty());
    assertEquals(mcpSchemaIds.size(), 3);
    assertTrue(mcpSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1.getSchemaId()));
    assertTrue(
        mcpSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1_FIX.getSchemaId()));
    assertTrue(mcpSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL.getSchemaId()));

    var mclSchemaIds =
        schemaData.getSchemaIdsForSchemaName(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    assertFalse(mclSchemaIds.isEmpty());
    assertEquals(mclSchemaIds.size(), 6);
    assertTrue(mclSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1.getSchemaId()));
    assertTrue(mclSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_LOG.getSchemaId()));
    assertTrue(
        mclSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1.getSchemaId()));
    assertTrue(mclSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES.getSchemaId()));
    assertTrue(mclSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1_FIX.getSchemaId()));
    assertTrue(
        mclSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1_FIX.getSchemaId()));

    var singleVersionSchemaIds =
        schemaData.getSchemaIdsForSchemaName(EventUtils.METADATA_CHANGE_EVENT_SCHEMA_NAME);
    assertFalse(singleVersionSchemaIds.isEmpty());
    assertEquals(singleVersionSchemaIds.size(), 3);
    assertTrue(
        singleVersionSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1.getSchemaId()));
    assertTrue(
        singleVersionSchemaIds.contains(
            SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1_FIX.getSchemaId()));
    assertTrue(
        singleVersionSchemaIds.contains(SchemaIdOrdinal.METADATA_CHANGE_EVENT.getSchemaId()));
  }

  @Test
  public void testGetSchemaNameForSchemaId() {
    // Test that we can get schema names for known schema IDs
    EventSchemaData schemaData = new EventSchemaData();

    var mcpSchemaName =
        schemaData.getSchemaNameForSchemaId(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL.getSchemaId());
    assertNotNull(mcpSchemaName);
    assertEquals(mcpSchemaName, EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);

    var mclSchemaName =
        schemaData.getSchemaNameForSchemaId(SchemaIdOrdinal.METADATA_CHANGE_LOG.getSchemaId());
    assertNotNull(mclSchemaName);
    assertEquals(mclSchemaName, EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);

    var duheSchemaName =
        schemaData.getSchemaNameForSchemaId(
            SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT.getSchemaId());
    assertNotNull(duheSchemaName);
    assertEquals(duheSchemaName, EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);
  }

  @Test
  public void testGetSchemaNameForSchemaIdWithUnknownId() {
    // Test that unknown schema IDs return empty
    EventSchemaData schemaData = new EventSchemaData();
    var unknownSchemaName = schemaData.getSchemaNameForSchemaId(999);
    assertNull(unknownSchemaName);
  }

  @Test
  public void testGetSchemaIdsForSchemaNameWithUnknownName() {
    // Test that unknown schema names return empty
    EventSchemaData schemaData = new EventSchemaData();
    var unknownSchemaIds = schemaData.getSchemaIdsForSchemaName("UnknownSchema");
    assertTrue(unknownSchemaIds.isEmpty());
  }

  @Test
  public void testGetSchemaIdsForSchemaNameWithNull() {
    // Test that null schema names return empty
    EventSchemaData schemaData = new EventSchemaData();
    var nullSchemaIds = schemaData.getSchemaIdsForSchemaName(null);
    assertTrue(nullSchemaIds.isEmpty());
  }

  @Test
  public void testGetAllSchemaNames() {
    // Test that we can get all schema names
    EventSchemaData schemaData = new EventSchemaData();
    List<String> allSchemaNames = new ArrayList<>(schemaData.getSchemaNameToIdsMap().keySet());
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
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1.getSchemaId(), 0);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1.getSchemaId(), 1);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1.getSchemaId(), 2);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1.getSchemaId(), 3);
    assertEquals(SchemaIdOrdinal.PLATFORM_EVENT.getSchemaId(), 4);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1.getSchemaId(), 5);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1.getSchemaId(), 6);
    assertEquals(SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1.getSchemaId(), 7);
    assertEquals(SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT.getSchemaId(), 8);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1_FIX.getSchemaId(), 9);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL_V1_FIX.getSchemaId(), 10);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG_V1_FIX.getSchemaId(), 11);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES_V1_FIX.getSchemaId(), 12);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_EVENT_V1_FIX.getSchemaId(), 13);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT_V1_FIX.getSchemaId(), 14);
    assertEquals(SchemaIdOrdinal.METADATA_AUDIT_EVENT_V1_FIX.getSchemaId(), 15);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL.getSchemaId(), 16);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_PROPOSAL.getSchemaId(), 17);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG.getSchemaId(), 18);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_LOG_TIMESERIES.getSchemaId(), 19);
    assertEquals(SchemaIdOrdinal.METADATA_CHANGE_EVENT.getSchemaId(), 20);
    assertEquals(SchemaIdOrdinal.FAILED_METADATA_CHANGE_EVENT.getSchemaId(), 21);
    assertEquals(SchemaIdOrdinal.METADATA_AUDIT_EVENT.getSchemaId(), 22);
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
    EventSchemaData schemaData = new EventSchemaData();
    Map<String, List<Integer>> schemaMap = schemaData.getSchemaNameToIdsMap();
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
    EventSchemaData schemaData = new EventSchemaData();
    Map<Integer, String> schemaMap = schemaData.getSchemaIdToNameMap();
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
    assertTrue(schemaMap.containsKey(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL_V1.getSchemaId()));
    assertTrue(schemaMap.containsKey(SchemaIdOrdinal.METADATA_CHANGE_PROPOSAL.getSchemaId()));
    assertTrue(schemaMap.containsKey(SchemaIdOrdinal.DATAHUB_UPGRADE_HISTORY_EVENT.getSchemaId()));
    assertTrue(schemaMap.containsKey(SchemaIdOrdinal.PLATFORM_EVENT.getSchemaId()));
  }

  @Test
  public void testSchemaNameToSchemaIdsMappingConsistency() {
    // Test that schema name to schema IDs mapping is consistent
    EventSchemaData schemaData = new EventSchemaData();
    Map<String, List<Integer>> nameToIdsMap = schemaData.getSchemaNameToIdsMap();
    Map<Integer, String> idToNameMap = schemaData.getSchemaIdToNameMap();

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
    EventSchemaData schemaData = new EventSchemaData();
    Map<String, List<Integer>> nameToIdsMap = schemaData.getSchemaNameToIdsMap();
    Map<Integer, String> idToNameMap = schemaData.getSchemaIdToNameMap();

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
    EventSchemaData schemaData = new EventSchemaData();
    List<String> allSchemaNames = new ArrayList<>(schemaData.getSchemaNameToIdsMap().keySet());
    Map<String, List<Integer>> nameToIdsMap = schemaData.getSchemaNameToIdsMap();

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
    EventSchemaData schemaData = new EventSchemaData();
    Map<Integer, String> idToNameMap = schemaData.getSchemaIdToNameMap();
    Map<String, List<Integer>> nameToIdsMap = schemaData.getSchemaNameToIdsMap();

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
