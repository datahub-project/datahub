package com.linkedin.gms.factory.kafka.schemaregistry;

import java.util.Map;
import lombok.Data;

@Data
public class SchemaRegistryConfig {
  private final Class<?> serializer;
  private final Class<?> deserializer;
  private final Map<String, Object> properties;
}
