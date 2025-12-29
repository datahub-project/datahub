package io.datahubproject.openapi.v3.models;

import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class FacetMetadata {
  private String field;
  private Map<String, Long> aggregations;
}
