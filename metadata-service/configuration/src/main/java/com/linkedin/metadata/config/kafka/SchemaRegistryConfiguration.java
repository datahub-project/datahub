package com.linkedin.metadata.config.kafka;

import lombok.Data;

@Data
public class SchemaRegistryConfiguration {
  private String type;

  private String url;
}
