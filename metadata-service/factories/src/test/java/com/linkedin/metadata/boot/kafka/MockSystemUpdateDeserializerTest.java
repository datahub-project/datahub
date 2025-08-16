package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.EventSchemaConstants;
import com.linkedin.metadata.EventUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
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
    // Verify that DUHE uses the correct schema version
    int expectedVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);
    assertEquals(expectedVersion, 1);
  }

  @Test
  public void testMockSchemaRegistryClient2Behavior() {
    // Test the custom MockSchemaRegistryClient2 behavior
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, "duhe-topic");
    configs.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "3");

    deserializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = deserializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);

    // Test that getSchemaById returns the schema for the configured schemaId
    try {
      ParsedSchema schema = schemaRegistry.getSchemaById(1); // Request ID 1
      assertNotNull(schema); // Should return schema for ID 3 (configured schemaId)
    } catch (IOException | RestClientException e) {
      fail("Should not throw exception: " + e.getMessage());
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
