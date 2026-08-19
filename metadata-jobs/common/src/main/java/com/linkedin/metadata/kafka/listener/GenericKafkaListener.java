package com.linkedin.metadata.kafka.listener;

import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import com.linkedin.metadata.kafka.context.inbound.InboundBatchAffinityResolver;
import com.linkedin.metadata.kafka.context.inbound.InboundContextResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Generic interface for Kafka and pgQueue listeners that process events with hooks.
 *
 * @param <E> The event type the hook processes
 * @param <H> The hook type this listener processes
 * @param <R> The raw record / payload type
 */
public interface GenericKafkaListener<E, H extends EventHook<E>, R> {

  /**
   * Initializes the listener with system context and hooks.
   *
   * @param systemOperationContext base operation context, used as the resolver baseline and for
   *     metric / span scaffolding
   * @param consumerGroup consumer group identifier
   * @param hooks hooks to invoke per event
   * @param fineGrainedLoggingEnabled whether to attach per-event attributes to logs / spans
   * @param aspectsToDrop entity-type → set of aspect names to skip
   * @param inboundContextResolver resolver that turns an inbound envelope into the per-event {@link
   *     OperationContext}
   * @param batchAffinityResolver resolver that partitions an inbound batch into affinity slices for
   *     batch dispatch; single-bean (deployments that want affinity-aware splitting register their
   *     own {@code @Component}; otherwise the {@code @ConditionalOnMissingBean} OSS default loads).
   *     Single-event listeners ignore this field; batch listeners read it in {@code consumeBatch}.
   * @return this listener instance for chaining
   */
  GenericKafkaListener<E, H, R> init(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull String consumerGroup,
      @Nonnull List<H> hooks,
      boolean fineGrainedLoggingEnabled,
      @Nonnull Map<String, Set<String>> aspectsToDrop,
      @Nonnull InboundContextResolver inboundContextResolver,
      @Nonnull InboundBatchAffinityResolver batchAffinityResolver);

  /** Process a batch of Kafka consumer records. */
  void consumeBatch(@Nonnull List<ConsumerRecord<String, R>> consumerRecords);

  /**
   * Default Kafka entry: wraps the consumer record in a transport-neutral envelope and delegates to
   * {@link #consumeEnvelope(InboundMetadataEnvelope)}.
   */
  default void consume(@Nonnull final ConsumerRecord<String, R> consumerRecord) {
    consumeEnvelope(InboundMetadataEnvelope.fromKafka(consumerRecord, getConsumerGroupId()));
  }

  /** Unified entry point. Both Kafka and pgQueue paths funnel through this method. */
  void consumeEnvelope(@Nonnull InboundMetadataEnvelope<R> envelope);

  /**
   * Converts a raw record / payload to the specific event type.
   *
   * @param record the raw payload to convert
   * @return the converted event object
   */
  E convertRecord(@Nonnull R record) throws IOException;

  /** Returns the consumer group ID for this listener. */
  @Nonnull
  String getConsumerGroupId();

  /** Returns the hooks used by this listener. */
  @Nonnull
  List<H> getHooks();
}
