package io.datahubproject.openapi.v3.models;

import io.datahubproject.openapi.models.GenericEntityScrollResult;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericEntityScrollResultV3
    implements GenericEntityScrollResult<GenericAspectV3, GenericEntityV3> {
  private String scrollId;
  private List<GenericEntityV3> entities;
}
