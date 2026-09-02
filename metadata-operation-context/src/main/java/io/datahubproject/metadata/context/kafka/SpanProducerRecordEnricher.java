package io.datahubproject.metadata.context.kafka;

import io.opentelemetry.sdk.trace.data.SpanData;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Enriches an outbound Kafka record from attributes captured on its parent span. */
public interface SpanProducerRecordEnricher {
  void enrichHeader(@Nonnull ProducerRecord<?, ?> record, @Nonnull SpanData parentSpan);
}
