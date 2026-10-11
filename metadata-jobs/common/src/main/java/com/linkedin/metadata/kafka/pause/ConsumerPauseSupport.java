package com.linkedin.metadata.kafka.pause;

import javax.annotation.Nonnull;

/** Pause and resume message consumption for a logical listener id (e.g. Kafka container id). */
public interface ConsumerPauseSupport {

  void pause(@Nonnull String listenerContainerId);

  void resume(@Nonnull String listenerContainerId);
}
