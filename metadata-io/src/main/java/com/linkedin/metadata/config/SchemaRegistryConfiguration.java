package com.linkedin.metadata.config;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;


@Data
@AllArgsConstructor
public class SchemaRegistryConfiguration {

  @Setter(AccessLevel.NONE)
  final private String type;

  @Setter(AccessLevel.NONE)
  final private String url;

}
