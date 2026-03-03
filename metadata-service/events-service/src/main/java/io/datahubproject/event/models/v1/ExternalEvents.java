package io.datahubproject.event.models.v1;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ExternalEvents {
  /** Offset id for the next batch */
  private String offsetId;

  /** Count of the events */
  private Long count;

  /** The raw events */
  private List<ExternalEvent> events;
}
