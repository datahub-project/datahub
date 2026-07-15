package com.linkedin.metadata.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.Test;

public class QueueModelTest {

  @Test
  public void queueMessageHeader_copiesValueBytes() {
    byte[] value = new byte[] {1, 2, 3};
    QueueMessageHeader header = new QueueMessageHeader("k", value);
    value[0] = 9;
    assertEquals(header.value()[0], (byte) 1);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void queueMessageHeader_rejectsNullKey() {
    new QueueMessageHeader(null, new byte[0]);
  }

  @Test
  public void queueTopicMetadata_validatePriority() {
    QueueTopicMetadata.validatePriority(QueueTopicMetadata.DEFAULT_PRIORITY);
    expectThrows(IllegalArgumentException.class, () -> QueueTopicMetadata.validatePriority(-1));
    expectThrows(IllegalArgumentException.class, () -> QueueTopicMetadata.validatePriority(10));
  }

  @Test
  public void priorityBand_containsAndValidates() {
    PriorityBand band = new PriorityBand(2, 5, 3);
    assertTrue(band.contains(3));
    assertFalse(band.contains(6));
    expectThrows(IllegalArgumentException.class, () -> new PriorityBand(5, 2, 1));
    expectThrows(IllegalArgumentException.class, () -> new PriorityBand(0, 1, 0));
  }

  @Test
  public void queueTopicMetadata_recordAccessors() {
    QueueTopicMetadata meta = new QueueTopicMetadata(42L, 4, Optional.of(7));
    assertEquals(meta.id(), 42L);
    assertEquals(meta.partitionCount(), 4);
    assertEquals(meta.defaultContentTypeId(), Optional.of(7));
  }

  @Test
  public void consumerRegistrationRow_holdsValues() {
    Instant now = Instant.parse("2026-01-01T00:00:00Z");
    ConsumerRegistrationRow row = new ConsumerRegistrationRow("grp", 9L, now, now);
    assertEquals(row.consumerGroup(), "grp");
    assertEquals(row.topicId(), 9L);
  }

  @Test
  public void enqueueBatchItem_record() {
    EnqueueBatchItem item =
        new EnqueueBatchItem(
            "topic",
            "rk",
            5,
            "x".getBytes(StandardCharsets.UTF_8),
            Optional.of("application/json"),
            List.of(new QueueMessageHeader("h", new byte[] {1})),
            PgQueuePayloadCompression.NONE);
    assertEquals(item.topicName(), "topic");
    assertEquals(item.priority(), 5);
  }
}
