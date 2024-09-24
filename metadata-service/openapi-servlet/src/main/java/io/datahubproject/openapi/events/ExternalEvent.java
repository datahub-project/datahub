package io.datahubproject.openapi.events;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Schema(description = "An external event")
@Data
@Getter
@Setter
public class ExternalEvent {
  @Schema(description = "The encoding type of the event", example = "application/json")
  private String contentType;

  @Schema(description = "The raw serialized event itself")
  private String value;
}
