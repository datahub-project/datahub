package com.linkedin.metadata.kafka.context.inbound;

import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Plugin point for enriching the per-event {@link OperationContext} with information derived from
 * an inbound message.
 *
 * <p>Implementations transform the base {@link OperationContext} using fields from {@link
 * InboundMetadataEnvelope} — the transport-neutral view that Kafka and pgQueue listeners both
 * populate before dispatching to hooks. Typical use cases: identification, security context setup,
 * request tracing.
 *
 * <p>Composed by {@link InboundContextResolver} in {@link org.springframework.core.annotation.Order
 * Spring order}. OSS ships with no enricher beans — the resolver becomes a pass-through. Downstream
 * distributions register their own beans without modifying OSS.
 *
 * <p>Lives in {@code metadata-jobs/common} rather than {@code metadata-operation-context} to avoid
 * forcing the {@link InboundMetadataEnvelope} transitive (and the kafka-clients chain it depends
 * on) onto every consumer of the foundational operation-context module.
 *
 * <p><b>Asymmetry with {@code OutboundContextEnricher}:</b> inbound enrichers are <b>functional</b>
 * — they take a base {@link OperationContext} and return a new (or the same) enriched context.
 * Outbound enrichers are <b>imperative</b> — they mutate the {@code ProducerRecord} in place. The
 * patterns are intentionally different because the values they operate on are different: {@link
 * OperationContext} is an immutable value, while a Kafka {@code ProducerRecord} is a mutable
 * builder that's about to be sent. Do not assume the inbound pattern from looking at the outbound
 * interface, or vice-versa.
 */
public interface InboundContextEnricher {

  /**
   * Enrich the {@link OperationContext} using a transport-neutral inbound message envelope. Default
   * implementation is a no-op; override when this enricher actually mutates the context.
   *
   * <p>Implementations return a new {@link OperationContext} derived from {@code baseContext} — the
   * base context itself is immutable and must not be mutated. Throwing from this method does not
   * abort the chain: {@link InboundContextResolver} catches per-enricher failures, logs, and
   * continues with the previous context.
   */
  @Nonnull
  default OperationContext enrich(
      @Nonnull final InboundMetadataEnvelope<?> envelope,
      @Nonnull final OperationContext baseContext) {
    return baseContext;
  }
}
