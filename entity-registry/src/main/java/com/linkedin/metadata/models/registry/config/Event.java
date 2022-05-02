package com.linkedin.metadata.models.registry.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event {
  String name;
  String doc;
}
