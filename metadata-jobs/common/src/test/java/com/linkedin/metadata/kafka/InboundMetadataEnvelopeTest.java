package com.linkedin.metadata.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.testng.annotations.Test;

public class InboundMetadataEnvelopeTest {

  @Test
  public void testFromPgQueueMapsAllFields() {
    Instant enqueuedAt = Instant.parse("2026-05-14T12:00:00Z");
    QueueMessageHandle handle = new QueueMessageHandle(99L, enqueuedAt, 7L, 2, 42L);
    byte[] payload = "test-payload".getBytes();
    QueueReceivedMessage message =
        new QueueReceivedMessage(
            handle,
            1,
            payload,
            Optional.of("application/vnd.apache.avro+binary"),
            PgQueuePayloadCompression.NONE,
            List.of(),
            "urn:li:dataset:abc",
            "lock-owner-1");

    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.fromPgQueue(
            message, "MetadataChangeLog_Versioned_v1", "mae-consumer", "decoded-value");

    assertEquals(envelope.getMessagingSystem(), MetricUtils.MESSAGING_SYSTEM_PGQUEUE);
    assertEquals(envelope.getLogicalTopic(), "MetadataChangeLog_Versioned_v1");
    assertEquals(envelope.getKey(), "urn:li:dataset:abc");
    assertEquals(envelope.getPayload(), "decoded-value");
    assertEquals(envelope.getEnqueuedAtMillis(), enqueuedAt.toEpochMilli());
    assertEquals(envelope.getConsumerGroupId(), "mae-consumer");
    assertEquals(envelope.getKafkaPartition().intValue(), 2);
    assertEquals(envelope.getKafkaOffset().longValue(), 42L);
    assertEquals(envelope.getSerializedValueSize().intValue(), payload.length);
    assertEquals(envelope.getTopicId().longValue(), 7L);
    assertEquals(envelope.getPartitionId().intValue(), 2);
    assertEquals(envelope.getEnqueueSeq().longValue(), 42L);
    assertEquals(envelope.getPriority().intValue(), 1);
    assertEquals(envelope.getMessageRowId().longValue(), 99L);
    assertEquals(envelope.getMessageEnqueuedAtMillis().longValue(), enqueuedAt.toEpochMilli());
    assertTrue(envelope.getHeaders().isEmpty());
  }

  @Test
  public void testFromKafkaPgQueueFieldsAreNull() {
    ConsumerRecord<String, String> record = new ConsumerRecord<>("MCP_v1", 0, 100L, "key", "value");

    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.fromKafka(record, "mce-consumer");

    assertEquals(envelope.getMessagingSystem(), MetricUtils.MESSAGING_SYSTEM_KAFKA);
    assertEquals(envelope.getLogicalTopic(), "MCP_v1");
    assertEquals(envelope.getKey(), "key");
    assertEquals(envelope.getPayload(), "value");
    assertEquals(envelope.getConsumerGroupId(), "mce-consumer");
    assertEquals(envelope.getKafkaPartition().intValue(), 0);
    assertEquals(envelope.getKafkaOffset().longValue(), 100L);
    assertNull(envelope.getTopicId());
    assertNull(envelope.getPartitionId());
    assertNull(envelope.getEnqueueSeq());
    assertNull(envelope.getMessageRowId());
    assertTrue(envelope.getHeaders().isEmpty());
  }

  @Test
  public void testFromKafkaCopiesHeadersLastWinsUtf8() {
    RecordHeaders headers = new RecordHeaders();
    headers.add("x-request-id", "first".getBytes(StandardCharsets.UTF_8));
    headers.add("x-request-id", "second".getBytes(StandardCharsets.UTF_8));
    headers.add("x-custom-header", "other-value".getBytes(StandardCharsets.UTF_8));

    ConsumerRecord<String, String> record =
        new ConsumerRecord<>(
            "MCP_v1",
            0,
            100L,
            0L,
            TimestampType.CREATE_TIME,
            0,
            0,
            "key",
            "value",
            headers,
            Optional.empty());

    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.fromKafka(record, "mce-consumer");

    assertEquals(
        envelope.getHeaders(), Map.of("x-request-id", "second", "x-custom-header", "other-value"));
  }

  @Test
  public void testFromKafkaSkipsNullHeaderValues() {
    RecordHeaders headers = new RecordHeaders();
    headers.add("x-keep", "ok".getBytes(StandardCharsets.UTF_8));
    headers.add("x-null", null);

    ConsumerRecord<String, String> record =
        new ConsumerRecord<>(
            "MCP_v1",
            0,
            100L,
            0L,
            TimestampType.CREATE_TIME,
            0,
            0,
            "key",
            "value",
            headers,
            Optional.empty());

    InboundMetadataEnvelope<String> envelope =
        InboundMetadataEnvelope.fromKafka(record, "mce-consumer");

    assertEquals(envelope.getHeaders(), Map.of("x-keep", "ok"));
  }
}
