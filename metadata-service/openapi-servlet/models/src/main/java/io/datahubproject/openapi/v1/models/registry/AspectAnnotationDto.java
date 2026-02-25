package io.datahubproject.openapi.v1.models.registry;

import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AspectAnnotationDto {
  private String name;
  private boolean timeseries;
  private boolean autoRender;
  private Map<String, Object> renderSpec;
}
