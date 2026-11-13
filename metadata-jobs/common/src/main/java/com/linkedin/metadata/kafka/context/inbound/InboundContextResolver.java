package com.linkedin.metadata.kafka.context.inbound;

import com.linkedin.metadata.kafka.InboundMetadataEnvelope;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Single chokepoint for deriving the per-event {@link OperationContext} of an inbound message.
 *
 * <p>The resolver takes the base (system) {@link OperationContext} plus the transport-neutral
 * {@link InboundMetadataEnvelope}, runs every registered {@link InboundContextEnricher} against
 * them in injection order, and returns the enriched result.
 *
 * <p>Why this exists: per-event routing, request tracing, and security context setup are
 * cross-cutting concerns that must attach to every Kafka / pgQueue message before hooks run.
 * Centralising them here keeps each listener implementation small and makes downstream
 * distributions a single-bean change away from full context propagation.
 *
 * <p>Registered as a Spring bean via {@code InboundContextResolverFactory}. OSS registers no
 * enricher beans by default — the resolver returns {@code baseContext} unchanged. Downstream
 * distributions add their own {@link InboundContextEnricher} beans without touching OSS.
 *
 * <p>Lives in {@code metadata-jobs/common} rather than {@code metadata-operation-context} to avoid
 * forcing the {@link InboundMetadataEnvelope} transitive (and the kafka-clients chain it depends
 * on) onto every consumer of the foundational operation-context module.
 */
@Slf4j
public class InboundContextResolver {

  private final List<InboundContextEnricher> enrichers;

  public InboundContextResolver(@Nonnull final List<InboundContextEnricher> enrichers) {
    this.enrichers = enrichers;
  }

  /**
   * Resolve the per-event {@link OperationContext} from a transport-neutral inbound envelope.
   *
   * <p>Each enricher invocation is isolated: if an enricher throws, the failure is logged and the
   * chain continues with the next enricher carrying the context as it was before the failure.
   * Matches the per-hook isolation pattern in {@code AbstractKafkaListener.processWithHooks} — one
   * broken enricher must not lose the message.
   */
  @Nonnull
  public OperationContext resolve(
      @Nonnull final InboundMetadataEnvelope<?> envelope,
      @Nonnull final OperationContext baseContext) {
    OperationContext current = baseContext;
    for (InboundContextEnricher enricher : enrichers) {
      try {
        current = enricher.enrich(envelope, current);
      } catch (Exception e) {
        log.error(
            "InboundContextEnricher {} failed; continuing chain with previous context",
            enricher.getClass().getSimpleName(),
            e);
      }
    }
    return current;
  }
}
