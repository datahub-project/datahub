package com.linkedin.metadata.boot.kafka;

import static com.linkedin.gms.factory.kafka.schemaregistry.SystemUpdateSchemaRegistryFactory.*;
import static org.testng.Assert.*;

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
    configs.put("schema.registry.url", "mock://test");
    configs.put("specific.avro.reader", "false");
  }

  @Test
  public void testConfigureWithValidConfigs() {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "16");
    deserializer.configure(configs, false);
    assertNotNull(deserializer.getSchemaRegistryClient());
  }

  @Test
  public void testConfigureWithMclTopic() {
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY, "mcl-topic");
    configs.put(MCL_VERSIONED_SCHEMA_REGISTRY_TOPIC_KEY + SYSTEM_UPDATE_TOPIC_KEY_ID_SUFFIX, "18");
    deserializer.configure(configs, false);
    assertNotNull(deserializer.getSchemaRegistryClient());
  }

  @Test
  public void testConfigureWithMissingIdSuffix() {
    configs.put(MCP_SCHEMA_REGISTRY_TOPIC_KEY, "mcp-topic");
    try {
      deserializer.configure(configs, false);
      fail("Expected exception when schema id is missing");
    } catch (NumberFormatException | NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testConfigureWithEmptyConfigs() {
    deserializer.configure(configs, false);
    assertNotNull(deserializer.getSchemaRegistryClient());
  }
}
