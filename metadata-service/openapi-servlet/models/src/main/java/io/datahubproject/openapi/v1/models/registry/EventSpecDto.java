package io.datahubproject.openapi.v1.models.registry;

import com.linkedin.metadata.models.EventSpec;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventSpecDto {
  private String name;
  private EventAnnotationDto eventAnnotation;
  private String pegasusSchemaName;
  private String pegasusSchemaDoc;

  public static EventSpecDto fromEventSpec(EventSpec eventSpec) {
    if (eventSpec == null) {
      return null;
    }

    return EventSpecDto.builder()
        .name(eventSpec.getName())
        .eventAnnotation(EventAnnotationDto.fromEventAnnotation(eventSpec.getEventAnnotation()))
        .pegasusSchemaName(
            eventSpec.getPegasusSchema() != null
                ? eventSpec.getPegasusSchema().getFullName()
                : null)
        .pegasusSchemaDoc(
            eventSpec.getPegasusSchema() != null ? eventSpec.getPegasusSchema().getDoc() : null)
        .build();
  }
}
