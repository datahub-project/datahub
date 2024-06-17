package io.datahubproject.openapi.models;

import java.util.Map;

public interface GenericEntity {
  String getUrn();

  Map<String, Object> getAspects();
}
