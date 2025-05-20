package com.linkedin.metadata.kafka.generic;

import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Generic interface for Kafka listeners that process events with hooks.
 *
 * @param <E> The event type the hook processes
 * @param <H> The hook type this listener processes
 * @param <R> The record type
 */
public interface GenericKafkaListener<E, H extends EventHook<E>, R> {

  /**
   * Initializes the listener with system context and hooks.
   *
   * @param systemOperationContext The operation context for the system
   * @param consumerGroup The consumer group ID
   * @param hooks The list of hooks to apply
   * @param fineGrainedLoggingEnabled Whether to enable fine-grained logging
   * @param aspectsToDrop Map of aspects to drop during processing
   * @return this listener instance for chaining
   */
  GenericKafkaListener<E, H, R> init(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull String consumerGroup,
      @Nonnull List<H> hooks,
      boolean fineGrainedLoggingEnabled,
      @Nonnull Map<String, Set<String>> aspectsToDrop);

  /**
   * Process a Kafka consumer record.
   *
   * @param consumerRecord The Kafka consumer record to process
   */
  void consume(@Nonnull ConsumerRecord<String, R> consumerRecord);

  /**
   * Converts a generic record to the specific event type.
   *
   * @param record The generic record to convert
   * @return The converted event object
   */
  E convertRecord(@Nonnull R record) throws IOException;

  /**
   * Gets the consumer group ID for this listener.
   *
   * @return The consumer group ID
   */
  @Nonnull
  String getConsumerGroupId();

  /**
   * Gets the list of hooks used by this listener.
   *
   * @return The list of hooks
   */
  @Nonnull
  List<H> getHooks();
}
