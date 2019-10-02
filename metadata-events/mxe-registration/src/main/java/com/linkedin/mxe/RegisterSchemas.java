package com.linkedin.mxe;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;


public class RegisterSchemas {

  public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

  private RegisterSchemas() {
  }

  static CachedSchemaRegistryClient createClient(String url) {
    return new CachedSchemaRegistryClient(url, Integer.MAX_VALUE);
  }

  private static void registerSchema(String topic, Schema schema, CachedSchemaRegistryClient client) {
    try {
      client.register(topic, schema);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void registerSchema(String topic, String schemaRegistryUrl) {
    CachedSchemaRegistryClient client = createClient(schemaRegistryUrl);
    Schema schema = Configs.TOPIC_SCHEMA_MAP.get(topic);
    System.out.println(String.format("Registering %s using registry %s (size: %d)", topic, schemaRegistryUrl,
        schema.toString(false).length()));
    registerSchema(topic, schema, client);
  }

  public static void main(final String[] args) {
    final String url = args.length == 1 ? args[0] : DEFAULT_SCHEMA_REGISTRY_URL;
    Configs.TOPIC_SCHEMA_MAP.forEach((topic, schema) -> {
      System.out.println(String.format("Registering %s using registry %s (size: %d)", topic,
          url, schema.toString(false).length()));
      registerSchema(topic, url);
    });
  }
}
