package io.datahubproject.openapi.models;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericScrollResult<T> {
  private String scrollId;
  private List<T> results;
}
