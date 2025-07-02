package io.datahubproject.openapi.v1.models.registry;

import com.linkedin.metadata.models.annotation.EventAnnotation;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventAnnotationDto {
  private String name;

  public static EventAnnotationDto fromEventAnnotation(EventAnnotation annotation) {
    if (annotation == null) {
      return null;
    }

    return EventAnnotationDto.builder().name(annotation.getName()).build();
  }
}
