package io.datahubproject.event;

import io.datahubproject.event.models.v1.ExternalEvents;
import javax.annotation.Nullable;

/** Backend for {@link ExternalEventsService} (Kafka or pgQueue). */
public interface ExternalEventsPollHandler {

  /**
   * @param physicalTopicName remapped Kafka/pgQueue topic name (not display name)
   */
  ExternalEvents poll(
      String physicalTopicName,
      @Nullable String offsetId,
      int limit,
      long timeoutMs,
      @Nullable Integer lookbackWindowDays)
      throws Exception;

  default void shutdown() {}
}
