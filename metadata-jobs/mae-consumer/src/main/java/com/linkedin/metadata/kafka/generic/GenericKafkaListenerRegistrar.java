package com.linkedin.metadata.kafka.generic;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.springframework.kafka.config.KafkaListenerEndpoint;

/**
 * Generic interface for registering Kafka listeners for different event types.
 *
 * @param <E> The event type
 * @param <H> The hook type
 * @param <R> The record type
 */
public interface GenericKafkaListenerRegistrar<E, H extends EventHook<E>, R> {

  /**
   * Gets the enabled hooks for this registrar.
   *
   * @return List of enabled hooks sorted by execution order
   */
  @Nonnull
  List<H> getEnabledHooks();

  /**
   * Registers a Kafka listener endpoint.
   *
   * @param kafkaListenerEndpoint The endpoint to register
   * @param startImmediately Whether to start the listener immediately
   */
  void registerKafkaListener(
      @Nonnull KafkaListenerEndpoint kafkaListenerEndpoint, boolean startImmediately);

  /**
   * Creates a listener endpoint for the given consumer group and topics.
   *
   * @param consumerGroupId The consumer group ID
   * @param topics List of topics to listen to
   * @param hooks List of hooks to apply to events
   * @return The created Kafka listener endpoint
   */
  @Nonnull
  KafkaListenerEndpoint createListenerEndpoint(
      @Nonnull String consumerGroupId, @Nonnull List<String> topics, @Nonnull List<H> hooks);

  /**
   * Builds a consumer group name from a suffix.
   *
   * @param suffix The suffix to append to the base consumer group name
   * @return The complete consumer group name
   */
  @Nonnull
  String buildConsumerGroupName(@Nonnull String suffix);

  /**
   * Creates a Kafka listener instance.
   *
   * @param consumerGroupId The consumer group ID
   * @param hooks The hooks to apply
   * @param fineGrainedLoggingEnabled Whether to enable fine-grained logging
   * @param aspectsToDrop Map of aspects to drop during processing
   * @return The created Kafka listener
   */
  @Nonnull
  GenericKafkaListener<E, H, R> createListener(
      @Nonnull String consumerGroupId,
      @Nonnull List<H> hooks,
      boolean fineGrainedLoggingEnabled,
      @Nonnull Map<String, Set<String>> aspectsToDrop);
}
