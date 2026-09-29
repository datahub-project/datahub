package com.linkedin.metadata.dao.producer.context.outbound;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Single chokepoint for writing {@link OperationContext} fields onto outbound messages.
 *
 * <p>Composes all registered {@link OutboundContextEnricher} instances. Producers call {@link
 * #apply} once per outbound record, immediately before {@code send(...)}. OSS registers no enricher
 * beans by default — calling {@link #apply} is a no-op. Downstream distributions register their own
 * enrichers to propagate routing / trace / security headers without touching OSS.
 *
 * <p>Lives in {@code metadata-dao-impl/kafka-producer} so it sits alongside {@link
 * com.linkedin.metadata.dao.producer.KafkaEventProducer KafkaEventProducer} and is reachable from
 * {@code metadata-service/factories} without creating a dependency cycle through {@code
 * metadata-jobs/common}.
 */
@Slf4j
public class OutboundContextResolver {

  private final List<OutboundContextEnricher> enrichers;

  public OutboundContextResolver(@Nonnull final List<OutboundContextEnricher> enrichers) {
    this.enrichers = enrichers;
  }

  /**
   * Apply every enricher to the outbound Kafka producer record. Mutates {@code record} in place.
   *
   * <p>Each enricher invocation is isolated: if an enricher throws, the failure is logged and the
   * chain continues with the next enricher. Matches the per-hook isolation pattern in {@code
   * AbstractKafkaListener.processWithHooks} — one broken enricher must not abort the produce.
   */
  public void apply(
      @Nonnull final ProducerRecord<?, ?> record, @Nonnull final OperationContext context) {
    for (OutboundContextEnricher enricher : enrichers) {
      try {
        enricher.enrich(record, context);
      } catch (Exception e) {
        log.error(
            "OutboundContextEnricher {} failed; continuing chain",
            enricher.getClass().getSimpleName(),
            e);
      }
    }
  }
}
