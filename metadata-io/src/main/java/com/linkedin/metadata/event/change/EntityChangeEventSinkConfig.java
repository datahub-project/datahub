package com.linkedin.metadata.event.change;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;


/**
 * Configuration provided to any {@link EntityChangeEventSink} at initalization time.
 */
@Data
@AllArgsConstructor
@Getter
public class EntityChangeEventSinkConfig {
  /**
   * Static configuration for a sink provided boot time configuration.
   */
  private final Map<String, Object> staticConfig;
}
