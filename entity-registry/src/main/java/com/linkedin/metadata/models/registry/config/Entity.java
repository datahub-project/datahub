package com.linkedin.metadata.models.registry.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Entity {
  String name;
  String doc;
  String keyAspect;
  List<String> aspects;

  @Nullable String category;
}
