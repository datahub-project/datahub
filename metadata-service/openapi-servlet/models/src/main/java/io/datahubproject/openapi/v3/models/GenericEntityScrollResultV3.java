package io.datahubproject.openapi.v3.models;

import io.datahubproject.openapi.models.GenericEntity;
import io.datahubproject.openapi.models.GenericEntityScrollResult;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericEntityScrollResultV3<T extends GenericEntity>
    implements GenericEntityScrollResult<T> {
  private String scrollId;
  private List<T> entities;
}
