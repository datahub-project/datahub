package com.linkedin.metadata.config.kafka;

import lombok.Data;

@Data
public class KafkaConfiguration {

  private String bootstrapServers;

  private ListenerConfiguration listener;

  private SchemaRegistryConfiguration schemaRegistry;

  private ProducerConfiguration producer;
}
