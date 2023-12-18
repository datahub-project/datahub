package io.datahubproject.openapi.v3.models;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericScrollResult {
  private String scrollId;
  private List<GenericEntity> entities;
}
