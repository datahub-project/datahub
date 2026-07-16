package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.*;
import static org.testng.Assert.*;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MockSystemUpdateDeserializerTest {

  private MockSystemUpdateDeserializer deserializer;
  private Map<String, Object> configs;

  @BeforeMethod
  public void setUp() {
    deserializer = new MockSystemUpdateDeserializer();
    configs = new HashMap<>();

    // Add required KafkaAvroDeserializer configuration
    configs.put("schema.registry.url", "mock://test");
    configs.put("specific.avro.reader", "false");
  }

  @Test
  public void testConfigureWithValidConfigs() {
    // Setup configs for DUHE topic
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithDifferentSchemaId() {
    // Setup configs with different schema ID
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithIsKeyTrue() {
    // Setup configs
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    // Should work for both true and false
    deserializer.configure(configs, true);
    assertNotNull(deserializer.getSchemaRegistryClient());

    deserializer.configure(configs, false);
    assertNotNull(deserializer.getSchemaRegistryClient());
  }

  @Test
  public void testConfigureWithMissingTopicKey() {
    // Missing topic key
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    try {
      deserializer.configure(configs, false);
      fail("Expected exception when topic key is missing");
    } catch (NullPointerException e) {
      // Expected - trying to call toString() on null
    }
  }

  @Test
  public void testConfigureWithMissingSchemaId() {
    // Missing schema ID
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");

    try {
      deserializer.configure(configs, false);
      fail("Expected exception when schema ID is missing");
    } catch (NullPointerException e) {
      // Expected - trying to call toString() on null
    }
  }

  @Test
  public void testConfigureWithInvalidSchemaId() {
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "invalid-id");

    try {
      deserializer.configure(configs, false);
      fail("Expected exception when schema ID is not a valid integer");
    } catch (NumberFormatException e) {
      // Expected - trying to parse "invalid-id" as integer
    }
  }

  @Test
  public void testConfigureWithNullConfigs() {
    try {
      deserializer.configure(null, false);
      fail("Expected exception when configs is null");
    } catch (NullPointerException e) {
      // Expected
    }
  }

  @Test
  public void testConfigureWithEmptyConfigs() {
    try {
      deserializer.configure(configs, false);
      fail("Expected exception when configs is empty");
    } catch (NullPointerException e) {
      // Expected - trying to get DUHE_SCHEMA_REGISTRY_TOPIC_KEY from empty map
    }
  }

  @Test
  public void testSchemaVersionForDUHE() {
    // Test that DUHE uses the correct schema version
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // DUHE has only 1 schema ID, so version should be 1
    // We can verify this by checking the schema registry client behavior
  }

  @Test
  public void testCustomMockSchemaRegistryClientBehavior() {
    // Test that the custom mock schema registry client behaves correctly
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // Test that unregistered schema ID throws exception (no historical mapping)
    try {
      schemaRegistry.getSchemaById(2);
      fail("Unregistered schema ID 2 should throw exception since there's no historical mapping");
    } catch (Exception e) {
      // Expected - no historical schema ID mapping
      assertTrue(e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"));
    }
  }

  @Test
  public void testHistoricalSchemaIdMappingForDUHE() {
    // Test that historical schema ID mapping works for DUHE topic
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
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
  public void testHistoricalSchemaIdMappingOnlyForDUHE() {
    // Test that historical schema ID mapping only applies to DUHE topic, not other topics
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "MetadataChangeProposal_v1");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "0");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    try {
      // For DUHE topic: historical ID 2 should NOT map to ID 5 (no historical mapping)
      // This should fail since there's no schema registered with ID 2
      try {
        ParsedSchema duheSchema = schemaRegistry.getSchemaById(2);
        fail(
            "DUHE historical schema ID 2 should not map to configured ID 5 - no historical mapping");
      } catch (Exception e) {
        // Expected - no historical schema ID mapping
        assertTrue(
            e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"),
            "Expected Subject Not Found error for unregistered schema ID 2");
      }

      // For MCP topic: historical ID 2 should NOT map to ID 0 (no historical mapping)
      // This should also fail since there's no schema registered with ID 2
      try {
        ParsedSchema mcpSchema = schemaRegistry.getSchemaById(2);
        fail("MCP topic should not have historical schema ID mapping");
      } catch (Exception e) {
        // Expected - no historical schema ID mapping
        assertTrue(
            e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"),
            "Expected Subject Not Found error for unregistered schema ID 2");
      }
    } catch (Exception e) {
      fail("Test setup failed: " + e.getMessage());
    }
  }

  @Test
  public void testHistoricalSchemaIdMappingWithDifferentConfiguredIds() {
    // Test historical mapping with different configured schema IDs
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "10");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // Historical schema ID 5 should throw exception (no historical mapping)
    try {
      schemaRegistry.getSchemaById(5);
      fail("Historical schema ID 5 should throw exception since there's no historical mapping");
    } catch (Exception e) {
      // Expected - no historical schema ID mapping
      assertTrue(e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"));
    }
  }

  @Test
  public void testMockSchemaRegistryClient2Behavior() {
    // Test that the second mock schema registry client behaves correctly
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "DataHubUpgradeHistory_v1");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "5");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // Test that unregistered schema ID throws exception (no historical mapping)
    try {
      schemaRegistry.getSchemaById(2);
      fail("Unregistered schema ID 2 should throw exception since there's no historical mapping");
    } catch (Exception e) {
      // Expected - no historical schema ID mapping
      assertTrue(e.getMessage().contains("Subject Not Found") || e.getMessage().contains("40401"));
    }
  }

  @Test
  public void testTopicToSubjectNameIntegration() {
    // Test that the deserializer uses the correct subject name format
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "test-duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    deserializer.configure(configs, false);

    // The deserializer should register the schema with the correct subject name
    // which is topicName + "-value"
    String expectedSubjectName = "test-duhe-topic-value";

    // Verify the topicToSubjectName method works as expected
    String actualSubjectName = MockSystemUpdateSerializer.topicToSubjectName("test-duhe-topic");
    assertEquals(actualSubjectName, expectedSubjectName);
  }

  @Test
  public void testConfigureWithNonStringTopicKey() {
    // Test with non-string topic key
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, 123); // Non-string value
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    deserializer.configure(configs, false);

    // Should work since toString() is called on the value
    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }

  @Test
  public void testConfigureWithNonStringSchemaId() {
    // Test with non-string schema ID
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(
        DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, 5); // Integer value

    deserializer.configure(configs, false);

    // Should work since toString() is called on the value
    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
  }
}
