package io.datahubproject.openapi.v1.models.registry;

import com.linkedin.data.DataMap;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.HashMap;
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
  private long schemaVersion;

  public static AspectAnnotationDto fromAspectAnnotation(AspectAnnotation annotation) {
    if (annotation == null) {
      return null;
    }

    return AspectAnnotationDto.builder()
        .name(annotation.getName())
        .timeseries(annotation.isTimeseries())
        .autoRender(annotation.isAutoRender())
        .renderSpec(convertDataMap(annotation.getRenderSpec()))
        .schemaVersion(annotation.getSchemaVersion())
        .build();
  }

  private static Map<String, Object> convertDataMap(DataMap dataMap) {
    if (dataMap == null) {
      return null;
    }
    Map<String, Object> result = new HashMap<>();
    dataMap.forEach(result::put);
    return result;
  }
}
