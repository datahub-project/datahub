package io.datahubproject.event.models.v1;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class ExternalEvent {
  /** The encoding type of the event */
  private String contentType;

  /** The raw serialized event itself */
  private String value;
}
