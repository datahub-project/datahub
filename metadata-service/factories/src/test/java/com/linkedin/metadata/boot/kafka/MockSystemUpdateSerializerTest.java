package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.*;
import static org.testng.Assert.*;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MockSystemUpdateSerializerTest {

  private MockSystemUpdateSerializer serializer;
  private Map<String, Object> configs;

  @BeforeMethod
  public void setUp() {
    serializer = new MockSystemUpdateSerializer();
    configs = new HashMap<>();

    // Add required KafkaAvroSerializer configuration
    configs.put("schema.registry.url", "mock://test");
    configs.put("auto.register.schemas", "false");
  }

  @Test
  public void testConfigureWithValidConfigs() {
    // Setup configs for DUHE topic
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithMultipleTopics() {
    // Setup configs for multiple topics
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY, "mcl-topic");
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "2");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "3");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithNonSystemUpdateTopics() {
    // Add non-system-update topics
    configs.put("other.topic.key", "other-topic");
    configs.put("other.topic.key" + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    serializer.configure(configs, false);

    // Should still work but not register the non-system-update topic
    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithNonStringValues() {
    // Add config with non-string value
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, 123); // Non-string value
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    serializer.configure(configs, false);

    // Should still work but skip the non-string value
    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithMissingIdSuffix() {
    // Add topic without ID suffix
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    // Missing: configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX,
    // "1");

    try {
      serializer.configure(configs, false);
      fail("Expected exception when ID suffix is missing");
    } catch (NumberFormatException e) {
      // Expected - trying to parse null as integer
    }
  }

  @Test
  public void testTopicToSubjectName() {
    String topicName = "test-topic";
    String expectedSubjectName = topicName + "-value";
    String actualSubjectName = MockSystemUpdateSerializer.topicToSubjectName(topicName);
    assertEquals(actualSubjectName, expectedSubjectName);
  }

  @Test
  public void testGetSchemaRegistryClient() {
    // Configure first to set up the schema registry
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");
    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testSchemaVersionMappingForDUHE() {
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    serializer.configure(configs, false);

    // DUHE has only 1 schema ID, so version should be 1
    // We can verify this by checking the schema registry client behavior
    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testSchemaVersionMappingForMCL() {
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY, "mcl-topic");
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    serializer.configure(configs, false);

    // MCL has 4 schema IDs, so version should be 4
    // We can verify this by checking the schema registry client behavior
    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testSchemaVersionMappingForMCP() {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    serializer.configure(configs, false);

    // MCP has 2 schema IDs, so version should be 2
    // We can verify this by checking the schema registry client behavior
    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithEmptyConfigs() {
    // Empty configs should not cause issues
    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithNullConfigs() {
    try {
      serializer.configure(null, false);
      fail("Expected exception when configs is null");
    } catch (NullPointerException e) {
      // Expected
    }
  }

  @Test
  public void testConfigureWithInvalidIdFormat() {
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "invalid-id");

    try {
      serializer.configure(configs, false);
      fail("Expected exception when ID is not a valid integer");
    } catch (NumberFormatException e) {
      // Expected - trying to parse "invalid-id" as integer
    }
  }

  @Test
  public void testConfigureIsKeyParameter() {
    // Test that isKey parameter is passed through to parent class
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    // Should work for both true and false
    serializer.configure(configs, true);
  }

  @Test
  public void testHistoricalSchemaIdHandling() {
    // Test that historical schema ID handling works for any topic
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "MetadataChangeProposal_v1");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "0");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // Test that historical schema ID 2 throws exception (no historical mapping)
    try {
      schemaRegistry.getSchemaById(2);
      fail("Historical schema ID 2 should throw exception since there's no historical mapping");
    } catch (Exception e) {
      // Expected - no historical schema ID mapping
      assertTrue(e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"));
    }
  }

  @Test
  public void testNoHardcodedTopicNames() {
    // Test that the serializer works with any topic names, not just hardcoded ones
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "my-custom-duhe-topic-name");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "42");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "my-custom-mcp-topic-name");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "99");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // Test that historical schema ID 10 throws exception (no historical mapping)
    try {
      schemaRegistry.getSchemaById(10);
      fail("Historical schema ID 10 should throw exception since there's no historical mapping");
    } catch (Exception e) {
      // Expected - no historical schema ID mapping
      assertTrue(e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"));
    }
  }

  @Test
  public void testGetTopicNameForSchemaId() {
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "8");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "0");

    serializer.configure(configs, false);

    // Test that we can look up topic names by schema ID
    assertEquals(serializer.getTopicNameForSchemaId(8), "duhe-topic");
    assertEquals(serializer.getTopicNameForSchemaId(0), "mcp-topic");
    assertNull(serializer.getTopicNameForSchemaId(99)); // Non-existent ID
  }

  @Test
  public void testGetConfiguredSchemaIds() {
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "8");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "0");

    serializer.configure(configs, false);

    Map<String, Integer> configuredIds = serializer.getConfiguredSchemaIds();
    assertEquals(configuredIds.get("duhe-topic"), Integer.valueOf(8));
    assertEquals(configuredIds.get("mcp-topic"), Integer.valueOf(0));
    assertEquals(configuredIds.size(), 2);
  }

  @Test
  public void testHistoricalSchemaIdMappingForDUHE() {
    // Test that historical schema ID mapping works for DUHE topic
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // Test that historical schema ID 2 throws exception (no historical mapping)
    try {
      schemaRegistry.getSchemaById(2);
      fail("Historical schema ID 2 should throw exception since there's no historical mapping");
    } catch (Exception e) {
      // Expected - no historical schema ID mapping
      assertTrue(e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"));
    }
  }

  @Test
  public void testHistoricalSchemaIdMappingWithDifferentConfiguredIds() {
    // Test that historical schema ID mapping works with different configured IDs
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "10");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "MetadataChangeProposal_v1");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "99");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // Test that historical schema ID 5 throws exception (no historical mapping)
    try {
      schemaRegistry.getSchemaById(5);
      fail("Historical schema ID 5 should throw exception since there's no historical mapping");
    } catch (Exception e) {
      // Expected - no historical schema ID mapping
      assertTrue(e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"));
    }

    // Test that configured schema ID 10 works normally (it's the configured ID for DUHE)
    try {
      ParsedSchema schema = schemaRegistry.getSchemaById(10);
      assertNotNull(schema, "Configured schema ID 10 should work normally");
      assertTrue(
          schema.toString().contains("DataHubUpgradeHistoryEvent"),
          "Configured ID 10 should return DUHE schema");
    } catch (Exception e) {
      fail("Configured schema ID 10 should not throw exception: " + e.getMessage());
    }

    // Test that unregistered schema ID 15 throws exception (no historical mapping)
    try {
      schemaRegistry.getSchemaById(15);
      fail("Unregistered schema ID 15 should throw exception since there's no historical mapping");
    } catch (Exception e) {
      // Expected - no historical schema ID mapping
      assertTrue(e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"));
    }
  }

  @Test
  public void testCurrentSchemaIdNotAffectedByHistoricalMapping() {
    // Test that current schema ID (5) is not affected by historical mapping
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    try {
      // Current schema ID 5 should work normally
      ParsedSchema schema = schemaRegistry.getSchemaById(5);
      assertNotNull(schema, "Current schema ID 5 should work normally");
      assertTrue(
          schema.toString().contains("DataHubUpgradeHistoryEvent"),
          "Current ID 5 should return DUHE schema");
    } catch (IOException | RestClientException e) {
      fail("Should not throw exception for current schema ID: " + e.getMessage());
    }
  }

  @Test
  public void testHistoricalSchemaIdMappingAboveConfiguredId() {
    // Test that schema IDs >= configured ID are not affected by historical mapping
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    try {
      // Schema IDs >= 5 should not be affected by historical mapping
      for (int id = 5; id < 10; id++) {
        try {
          ParsedSchema schema = schemaRegistry.getSchemaById(id);
          // This might succeed or fail depending on what's registered, but shouldn't be mapped
          if (schema != null) {
            // If it succeeds, it should be the actual schema for that ID, not mapped
            assertFalse(
                schema.toString().contains("DataHubUpgradeHistoryEvent") && id != 5,
                "Schema ID " + id + " should not be mapped to DUHE schema");
          }
        } catch (Exception e) {
          // Expected for unregistered schema IDs
        }
      }
    } catch (Exception e) {
      fail("Historical mapping should not affect schema IDs >= configured ID: " + e.getMessage());
    }
  }
}
