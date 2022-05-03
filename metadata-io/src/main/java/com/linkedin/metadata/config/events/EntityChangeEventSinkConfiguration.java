package com.linkedin.metadata.config.events;

import com.linkedin.metadata.event.change.EntityChangeEventSink;
import java.util.Map;
import lombok.Data;


@Data
public class EntityChangeEventSinkConfiguration {
  /**
   * A fully-qualified class name for the {@link EntityChangeEventSink} implementation to be registered.
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
