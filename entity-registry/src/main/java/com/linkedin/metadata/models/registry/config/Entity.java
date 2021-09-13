package com.linkedin.metadata.models.registry.config;

import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;


@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
public class Entity {
  String name;
  String keyAspect;
  List<String> aspects;
}
