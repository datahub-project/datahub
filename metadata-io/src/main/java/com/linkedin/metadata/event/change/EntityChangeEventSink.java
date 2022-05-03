package com.linkedin.metadata.event.change;

import com.linkedin.metadata.event.EventSink;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import javax.annotation.Nonnull;


/**
 * An {@link EventSink} responsible for sinking change events to external systems.
 */
public interface EntityChangeEventSink extends EventSink<EntityChangeEvent> {
  /**
   * Sink a change event to an external sink.
   *
   * @param cfg a set of static configurations provided to the change event sink.
   */
  void init(@Nonnull final EntityChangeEventSinkConfig cfg);
}
