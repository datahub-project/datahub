package io.datahubproject.openapi.v2.models;

import io.datahubproject.openapi.models.GenericEntityScrollResult;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericEntityScrollResultV2
    implements GenericEntityScrollResult<GenericAspectV2, GenericEntityV2> {
  private String scrollId;
  private List<GenericEntityV2> results;
}
