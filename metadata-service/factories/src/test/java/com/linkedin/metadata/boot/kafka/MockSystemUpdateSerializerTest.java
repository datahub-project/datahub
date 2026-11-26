package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.*;
import static org.testng.Assert.*;

import io.confluent.kafka.schemaregistry.ParsedSchema;
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
    configs.put("schema.registry.url", "mock://test");
    configs.put("auto.register.schemas", "false");
  }

  @Test
  public void testConfigureWithMcpAndMclTopics() {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "16");
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY, "mcl-topic");
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "18");

    serializer.configure(configs, false);

    SchemaRegistryClient schemaRegistry = serializer.getSchemaRegistryClient();
    assertNotNull(schemaRegistry);
    assertEquals(serializer.getTopicNameForSchemaId(16), "mcp-topic");
    assertEquals(serializer.getTopicNameForSchemaId(18), "mcl-topic");
    assertNull(serializer.getTopicNameForSchemaId(8));
  }

  @Test
  public void testConfigureWithNonStringValues() {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, 123);
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "1");
    serializer.configure(configs, false);
    assertNotNull(serializer.getSchemaRegistryClient());
  }

  @Test
  public void testConfigureWithMissingIdSuffix() {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    try {
      serializer.configure(configs, false);
      fail("Expected exception when ID suffix is missing");
    } catch (NumberFormatException | NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testTopicToSubjectName() {
    assertEquals(MockSystemUpdateSerializer.topicToSubjectName("test-topic"), "test-topic-value");
  }

  @Test
  public void testConfiguredSchemaIdResolves() throws Exception {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "16");
    serializer.configure(configs, false);
    ParsedSchema schema = serializer.getSchemaRegistryClient().getSchemaById(16);
    assertNotNull(schema);
    assertTrue(schema.toString().contains("MetadataChangeProposal"));
  }

  @Test
  public void testGetConfiguredSchemaIds() {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "16");
    serializer.configure(configs, false);
    Map<String, Integer> configuredIds = serializer.getConfiguredSchemaIds();
    assertEquals(configuredIds.get("mcp-topic"), Integer.valueOf(16));
    assertEquals(configuredIds.size(), 1);
  }
}
