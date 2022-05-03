package com.linkedin.metadata.event;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


/**
 * An event sink is a SaaS-only component for mirroring
 * internal DataHub events into an external system, such as Amazon EventBridge.
 */
public interface EventSink<T extends RecordTemplate> {
  /**
   * Sink an event to an external sink.
   *
   * @param event a change event to sink to an external destination.
   */
  void sink(@Nonnull final T event);
}
