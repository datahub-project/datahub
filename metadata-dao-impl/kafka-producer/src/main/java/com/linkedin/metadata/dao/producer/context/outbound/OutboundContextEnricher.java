package com.linkedin.metadata.dao.producer.context.outbound;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Plugin point for writing fields from the current {@link OperationContext} onto an outbound
 * message before it is sent.
 *
 * <p>Typical use cases: writing the tenant ID, request ID, or trace headers onto outbound Kafka
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
 */
public interface OutboundContextEnricher {

  /**
   * Mutate an outbound Kafka producer record's headers using the current {@link OperationContext}.
   * Default implementation is a no-op.
   */
  default void enrich(
      @Nonnull final ProducerRecord<?, ?> record, @Nonnull final OperationContext context) {}
}
