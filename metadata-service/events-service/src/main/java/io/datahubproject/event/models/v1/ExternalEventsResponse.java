package io.datahubproject.event.models.v1;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Schema(description = "The response provided when fetching external events")
@Data
@Getter
@Setter
public class ExternalEventsResponse {
  @Schema(description = "Offset id the stream for scrolling", example = "0")
  private String offsetId;

  @Schema(description = "Count of the events", example = "100")
  private Long count;

  @Schema(description = "The raw events")
  private List<ExternalEvent> events;
}
