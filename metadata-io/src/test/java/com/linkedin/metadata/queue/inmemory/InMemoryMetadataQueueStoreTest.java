package com.linkedin.metadata.queue.inmemory;

import static org.testng.Assert.*;

import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueMessageHeader;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.Test;

public class InMemoryMetadataQueueStoreTest {

  private static final String TOPIC = "test-topic";
  private static final String GROUP = "test-group";
  private static final QueueTopicDefaults DEFAULTS =
      new QueueTopicDefaults(1, 3600, 100_000, 100_000_000, false, null);

  private static QueueMessageHandle enqueueOne(
      InMemoryMetadataQueueStore store, String key, int priority, byte[] payload) {
    return store.enqueue(
        TOPIC,
        key,
        DEFAULTS,
        priority,
        payload,
        Optional.empty(),
        List.of(),
        PgQueuePayloadCompression.NONE);
  }

  @Test
  public void enqueueAndReceive() {
    InMemoryMetadataQueueStore store = new InMemoryMetadataQueueStore();
    long topicId = store.ensureTopic(TOPIC, DEFAULTS);
    store.registerConsumer(GROUP, topicId);

    QueueMessageHandle handle =
        store.enqueue(
            TOPIC,
            "key-1",
            DEFAULTS,
            0,
            "hello".getBytes(),
            Optional.of("text/plain"),
            List.of(new QueueMessageHeader("tenant", "pw0".getBytes())),
            PgQueuePayloadCompression.NONE);

    assertNotNull(handle);
    List<QueueReceivedMessage> batch =
        store.receiveBatchForGroup(
            GROUP, topicId, List.of(0), "worker-1", Duration.ofMinutes(1), 10);

    assertEquals(batch.size(), 1);
    assertEquals(new String(batch.get(0).payload()), "hello");
    assertEquals(batch.get(0).contentType(), Optional.of("text/plain"));
    assertEquals(batch.get(0).headers().size(), 1);
    assertEquals(batch.get(0).headers().get(0).key(), "tenant");
    assertEquals(new String(batch.get(0).headers().get(0).value()), "pw0");
  }

  @Test
  public void commitAdvancesOffset() {
    InMemoryMetadataQueueStore store = new InMemoryMetadataQueueStore();
    long topicId = store.ensureTopic(TOPIC, DEFAULTS);
    store.registerConsumer(GROUP, topicId);

    QueueMessageHandle h1 = enqueueOne(store, "k1", 0, "msg1".getBytes());

    List<QueueReceivedMessage> batch1 =
        store.receiveBatchForGroup(GROUP, topicId, List.of(0), "w", Duration.ofMinutes(1), 10);
    assertEquals(batch1.size(), 1);

    store.commitForGroup(GROUP, List.of(h1), true);

    List<QueueReceivedMessage> batch2 =
        store.receiveBatchForGroup(GROUP, topicId, List.of(0), "w", Duration.ofMinutes(1), 10);
    assertEquals(batch2.size(), 0, "committed message should not be re-received");
  }

  @Test
  public void visibilityTimeoutRedelivers() {
    Instant now = Instant.parse("2026-01-01T00:00:00Z");
    InMemoryMetadataQueueStore store =
        new InMemoryMetadataQueueStore(Clock.fixed(now, ZoneId.of("UTC")));
    long topicId = store.ensureTopic(TOPIC, DEFAULTS);
    store.registerConsumer(GROUP, topicId);

    enqueueOne(store, "k1", 0, "data".getBytes());

    // Receive with a 30s visibility timeout.
    List<QueueReceivedMessage> first =
        store.receiveBatchForGroup(GROUP, topicId, List.of(0), "w", Duration.ofSeconds(30), 10);
    assertEquals(first.size(), 1);

    // Still within visibility window — should not redeliver.
    store.setClock(Clock.fixed(now.plusSeconds(10), ZoneId.of("UTC")));
    List<QueueReceivedMessage> during =
        store.receiveBatchForGroup(GROUP, topicId, List.of(0), "w", Duration.ofSeconds(30), 10);
    assertEquals(during.size(), 0, "message should be invisible during lease");

    // Advance past the 30s visibility timeout.
    store.setClock(Clock.fixed(now.plusSeconds(60), ZoneId.of("UTC")));
    List<QueueReceivedMessage> redelivered =
        store.receiveBatchForGroup(GROUP, topicId, List.of(0), "w2", Duration.ofSeconds(30), 10);
    assertEquals(redelivered.size(), 1, "message should be redelivered after visibility timeout");
  }

  @Test
  public void spoolPersistence() throws Exception {
    Path spoolFile = Files.createTempFile("queue-spool-", ".json");
    Files.deleteIfExists(spoolFile);

    InMemoryMetadataQueueStore store1 = new InMemoryMetadataQueueStore();
    long topicId = store1.ensureTopic(TOPIC, DEFAULTS);
    store1.registerConsumer(GROUP, topicId);
    enqueueOne(store1, "k1", 0, "persisted".getBytes());
    store1.saveTo(spoolFile);

    InMemoryMetadataQueueStore store2 = InMemoryMetadataQueueStore.withSpool(spoolFile);
    Optional<com.linkedin.metadata.queue.QueueTopicMetadata> meta = store2.fetchTopic(TOPIC);
    assertTrue(meta.isPresent(), "topic should survive spool round-trip");

    long topicId2 = meta.get().id();
    List<QueueReceivedMessage> batch =
        store2.receiveBatchForGroup(GROUP, topicId2, List.of(0), "w", Duration.ofMinutes(1), 10);
    assertEquals(batch.size(), 1);
    assertEquals(new String(batch.get(0).payload()), "persisted");

    Files.deleteIfExists(spoolFile);
  }

  @Test
  public void multipleConsumerGroupsIndependent() {
    InMemoryMetadataQueueStore store = new InMemoryMetadataQueueStore();
    long topicId = store.ensureTopic(TOPIC, DEFAULTS);
    store.registerConsumer("group-a", topicId);
    store.registerConsumer("group-b", topicId);

    QueueMessageHandle h = enqueueOne(store, "k1", 0, "shared".getBytes());

    List<QueueReceivedMessage> batchA =
        store.receiveBatchForGroup("group-a", topicId, List.of(0), "w", Duration.ofMinutes(1), 10);
    assertEquals(batchA.size(), 1);
    store.commitForGroup("group-a", List.of(h), true);

    // group-b has not consumed yet — should still see the message.
    List<QueueReceivedMessage> batchB =
        store.receiveBatchForGroup("group-b", topicId, List.of(0), "w", Duration.ofMinutes(1), 10);
    assertEquals(batchB.size(), 1, "group-b should independently see the message");
  }

  @Test
  public void priorityOrdering() {
    InMemoryMetadataQueueStore store = new InMemoryMetadataQueueStore();
    long topicId = store.ensureTopic(TOPIC, DEFAULTS);
    store.registerConsumer(GROUP, topicId);

    // Enqueue low-priority first, then high-priority.
    enqueueOne(store, "k1", 5, "low".getBytes());
    enqueueOne(store, "k2", 0, "high".getBytes());

    // Receive one at a time — high priority (0) should come first.
    List<QueueReceivedMessage> first =
        store.receiveBatchForGroup(GROUP, topicId, List.of(0), "w", Duration.ofMinutes(1), 1);
    assertEquals(first.size(), 1);
    assertEquals(new String(first.get(0).payload()), "high");

    store.commitForGroup(GROUP, List.of(first.get(0).handle()), true);

    List<QueueReceivedMessage> second =
        store.receiveBatchForGroup(GROUP, topicId, List.of(0), "w", Duration.ofMinutes(1), 1);
    assertEquals(second.size(), 1);
    assertEquals(new String(second.get(0).payload()), "low");
  }
}
