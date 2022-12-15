package com.linkedin.metadata.config;

import lombok.Data;


@Data
public class KafkaConfiguration {

  private String bootstrapServers;

  private SchemaRegistryConfiguration schemaRegistry;
}
