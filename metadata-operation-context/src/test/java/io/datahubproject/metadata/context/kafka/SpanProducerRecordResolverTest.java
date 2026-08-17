package io.datahubproject.metadata.context.kafka;

import static org.testng.Assert.assertEquals;

import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

public class SpanProducerRecordResolverTest {
  @Test
  public void appliesAllEnrichersAndIsolatesFailures() {
    SpanProducerRecordResolver resolver =
        new SpanProducerRecordResolver(
            List.of(
                (record, span) -> {
                  throw new IllegalStateException("broken");
                },
                (record, span) ->
                    record.headers().add("x-test", "value".getBytes(StandardCharsets.UTF_8))));
    ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");

    resolver.apply(record, org.mockito.Mockito.mock(SpanData.class));

    assertEquals(
        new String(record.headers().lastHeader("x-test").value(), StandardCharsets.UTF_8), "value");
  }
}
