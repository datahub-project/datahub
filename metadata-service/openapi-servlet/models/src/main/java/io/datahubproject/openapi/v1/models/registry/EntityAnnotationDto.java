package io.datahubproject.openapi.v1.models.registry;

import com.linkedin.metadata.models.annotation.EntityAnnotation;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntityAnnotationDto {
  private String name;
  private String keyAspect;
  private String searchGroup;
  private boolean viewUnrestricted;

  public static EntityAnnotationDto fromEntityAnnotation(EntityAnnotation annotation) {
    if (annotation == null) {
      return null;
    }

    return EntityAnnotationDto.builder()
        .name(annotation.getName())
        .keyAspect(annotation.getKeyAspect())
        .searchGroup(annotation.getSearchGroup())
        .viewUnrestricted(annotation.isViewUnrestricted())
        .build();
  }
}
