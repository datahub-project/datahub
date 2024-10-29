package io.datahubproject.openapi.models;

import java.util.Map;

public interface GenericEntity<T> {
  Map<String, T> getAspects();
}
