package com.linkedin.metadata.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
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
  }

  @Test
  public void testFromKafkaPgQueueFieldsAreNull() {
    org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record =
        new org.apache.kafka.clients.consumer.ConsumerRecord<>("MCP_v1", 0, 100L, "key", "value");

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
  }
}
