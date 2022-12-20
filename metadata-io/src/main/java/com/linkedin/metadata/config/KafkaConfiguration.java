package com.linkedin.metadata.config;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;


@Data
@AllArgsConstructor
public class KafkaConfiguration {

  @Setter(AccessLevel.NONE)
  private String bootstrapServers;

  @Setter(AccessLevel.NONE)
  private SchemaRegistryConfiguration schemaRegistry;
}
