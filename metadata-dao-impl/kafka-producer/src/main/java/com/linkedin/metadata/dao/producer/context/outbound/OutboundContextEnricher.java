package com.linkedin.metadata.dao.producer.context.outbound;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Plugin point for writing fields from the current {@link OperationContext} onto an outbound
 * message before it is sent.
 *
 * <p>Typical use cases: writing the routing ID, request ID, or trace headers onto outbound Kafka
 * records so the downstream consumer can resolve the same context.
 *
 * <p>Implementations mutate the outbound record in place. Each implementation overrides the
 * transport methods it cares about; defaults are no-ops.
 *
 * <p>Composed by {@link OutboundContextResolver} in {@link
 * org.springframework.core.annotation.Order Spring order}. OSS ships with no enricher beans — the
 * resolver is a no-op. Downstream distributions register their own beans without modifying OSS.
 *
 * <p>pgQueue outbound enrichment is deferred — pgQueue's producer API does not currently expose a
 * header-like field for context propagation. Add a method here when that lands.
 *
 * <p>Lives in {@code metadata-dao-impl/kafka-producer} so it sits alongside {@link
 * com.linkedin.metadata.dao.producer.KafkaEventProducer KafkaEventProducer} and is reachable from
 * {@code metadata-service/factories} without creating a dependency cycle through {@code
 * metadata-jobs/common}.
 *
 * <p><b>Asymmetry with {@code InboundContextEnricher}:</b> outbound enrichers are <b>imperative</b>
 * — they mutate the {@code ProducerRecord} in place. Inbound enrichers are <b>functional</b> — they
 * return a new {@link OperationContext}. The two interfaces operate on different kinds of values:
 * {@link OperationContext} is an immutable value (so enrichment has to return a new instance),
 * while a Kafka {@code ProducerRecord} is the mutable builder that's about to be handed to the
 * producer (so it's idiomatic to mutate it in place via {@code record.headers().add(...)}). Do not
 * assume the outbound pattern from looking at the inbound interface, or vice-versa.
 */
public interface OutboundContextEnricher {

  /**
   * Mutate an outbound Kafka producer record's headers using the current {@link OperationContext}.
   * Default implementation is a no-op.
   *
   * <p>Throwing from this method does not abort the produce: {@link OutboundContextResolver}
   * catches per-enricher failures, logs, and continues with the next enricher (the record may have
   * been partially mutated by this enricher before the throw — implementations should make each
   * header write idempotent or guard their own intermediate state).
   */
  default void enrich(
      @Nonnull final ProducerRecord<?, ?> record, @Nonnull final OperationContext context) {}
}
