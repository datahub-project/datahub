package io.datahubproject.metadata.context.kafka;

import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Applies all registered span-based enrichers to an outbound Kafka record. */
@Slf4j
public class SpanProducerRecordResolver {
  private final List<SpanProducerRecordEnricher> enrichers;

  public SpanProducerRecordResolver(@Nonnull final List<SpanProducerRecordEnricher> enrichers) {
    this.enrichers = enrichers;
  }

  public void apply(
      @Nonnull final ProducerRecord<?, ?> record, @Nonnull final SpanData parentSpan) {
    for (SpanProducerRecordEnricher enricher : enrichers) {
      try {
        enricher.enrichHeader(record, parentSpan);
      } catch (Exception e) {
        log.error(
            "SpanProducerRecordEnricher {} failed; continuing chain",
            enricher.getClass().getSimpleName(),
            e);
      }
    }
  }
}
