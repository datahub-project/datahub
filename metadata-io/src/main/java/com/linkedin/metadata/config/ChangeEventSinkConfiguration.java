package com.linkedin.metadata.config;

import java.util.Map;
import lombok.Data;


@Data
public class ChangeEventSinkConfiguration {
  /**
   * A fully-qualified class name for the {@link com.linkedin.metadata.event.change.ChangeEventSink} implementation to be registered.
   */
  private String type;

  /**
   * Whether the sink should be created (is enabled)
   */
  private boolean enabled;

  /**
   * A set of change-event-sink-specific configurations passed through during "init" of the sink.
   */
  private Map<String, Object> configs;
}
