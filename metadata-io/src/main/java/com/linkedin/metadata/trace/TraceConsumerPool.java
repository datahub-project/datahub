package com.linkedin.metadata.trace;

import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;

/** Borrows Kafka consumers for assign-only trace topic reads. */
public interface TraceConsumerPool {

  /**
   * Borrows a consumer, runs {@code action}, and returns the consumer to the pool in a {@code
   * finally} block.
   */
  <T> T withConsumer(@Nonnull String topic, @Nonnull TraceConsumerAction<T> action);

  /** Releases pooled resources on shutdown. */
  void shutdown();

  @FunctionalInterface
  interface TraceConsumerAction<T> {
    T execute(@Nonnull Consumer<String, GenericRecord> consumer) throws Exception;
  }
}
