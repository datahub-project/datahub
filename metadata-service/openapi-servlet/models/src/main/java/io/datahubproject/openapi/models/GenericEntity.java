package io.datahubproject.openapi.models;

import java.util.Map;

public interface GenericEntity<T> {
  String getUrn();

  Map<String, T> getAspects();
}
