package com.linkedin.metadata.pgqueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.Test;

public class PgQueuePollContextTest {

  @Test
  public void testCommitDelegatesToStore() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PgQueuePollContext ctx = new PgQueuePollContext(store, "grp1", Duration.ofSeconds(30), null);

    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.now(), 1L, 0, 10L);
    ctx.commit(List.of(handle));

    verify(store).commitForGroup(eq("grp1"), eq(List.of(handle)), eq(true));
  }

  @Test
  public void testCommitSkipsEmptyList() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PgQueuePollContext ctx = new PgQueuePollContext(store, "grp1", Duration.ofSeconds(30), null);

    ctx.commit(List.of());

    verify(store, never()).commitForGroup(anyString(), any(), any(Boolean.class));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testDecodeAvroWithoutDeserializerThrows() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PgQueuePollContext ctx = new PgQueuePollContext(store, "grp1", Duration.ofSeconds(30), null);

    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.now(), 1L, 0, 10L);
    QueueReceivedMessage msg =
        new QueueReceivedMessage(
            handle,
            0,
            new byte[] {1, 2, 3},
            Optional.empty(),
            PgQueuePayloadCompression.NONE,
            List.of(),
            "key",
            "owner");

    ctx.decodeAvro(msg, "topic");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDecodeAvroWithDeserializerDelegates() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    GenericRecord mockRecord = mock(GenericRecord.class);
    when(deserializer.deserialize(eq("topic"), any(byte[].class))).thenReturn(mockRecord);

    PgQueuePollContext ctx =
        new PgQueuePollContext(store, "grp1", Duration.ofSeconds(30), deserializer);

    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.now(), 1L, 0, 10L);
    QueueReceivedMessage msg =
        new QueueReceivedMessage(
            handle,
            0,
            new byte[] {1, 2, 3},
            Optional.empty(),
            PgQueuePayloadCompression.NONE,
            List.of(),
            "key",
            "owner");

    GenericRecord result = ctx.decodeAvro(msg, "topic");
    assertNotNull(result);
    assertEquals(result, mockRecord);
  }

  @Test
  public void testDecodeUtf8() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    PgQueuePollContext ctx = new PgQueuePollContext(store, "grp1", Duration.ofSeconds(30), null);

    byte[] payload = "hello world".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.now(), 1L, 0, 10L);
    QueueReceivedMessage msg =
        new QueueReceivedMessage(
            handle,
            0,
            payload,
            Optional.empty(),
            PgQueuePayloadCompression.NONE,
            List.of(),
            "key",
            "owner");

    String result = ctx.decodeUtf8(msg);
    assertEquals(result, "hello world");
  }

  @Test
  public void testGetters() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    Duration vis = Duration.ofSeconds(45);
    PgQueuePollContext ctx = new PgQueuePollContext(store, "my-group", vis, null);

    assertEquals(ctx.getConsumerGroupId(), "my-group");
    assertEquals(ctx.getVisibilityTimeout(), vis);
  }
}
