package com.linkedin.metadata.examples.configs;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class SchemaRegistryConfig {
  @Value("${SCHEMAREGISTRY_URL:http://localhost:8081}")
  private String schemaRegistryUrl;

  @Bean(name = "schemaRegistryClient")
  public SchemaRegistryClient schemaRegistryFactory() {
    return new CachedSchemaRegistryClient(schemaRegistryUrl, 512);
  }
}
