package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.EventSchemaConstants;
import com.linkedin.metadata.EventUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
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

    // DUHE should use version 1
    int expectedVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);
    assertEquals(expectedVersion, 1);
  }

  @Test
  public void testSchemaVersionMappingForMCL() {
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY, "mcl-topic");
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    serializer.configure(configs, false);

    // MCL should use version 1
    int expectedVersion =
        EventSchemaConstants.getLatestSchemaVersion(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    assertEquals(expectedVersion, 1);
  }

  @Test
  public void testSchemaVersionMappingForMCP() {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");

    serializer.configure(configs, false);

    // MCP should use version 2 (latest)
    int expectedVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    assertEquals(expectedVersion, 2);
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
    assertNotNull(serializer.getSchemaRegistryClient());

    serializer.configure(configs, false);
    assertNotNull(serializer.getSchemaRegistryClient());
  }
}
