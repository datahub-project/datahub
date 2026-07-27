package com.linkedin.metadata.pgqueue;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PartitionOffsetSkew;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

public class PgQueuePollWorkerTest {

  private static final String TOPIC = "test-topic";
  private static final String GROUP = "test-group";
  private static final QueueTopicMetadata TOPIC_META =
      new QueueTopicMetadata(1L, 3, Optional.empty());

  private PgQueueSetupOptions mockSetupOptions() {
    PgQueueSetupOptions opts = mock(PgQueueSetupOptions.class);
    when(opts.getTopicDefaultVisibilityTimeoutSeconds()).thenReturn(30);
    when(opts.getResolvedTopicCatalog()).thenReturn(Collections.emptyList());
    when(opts.getTopicDefaultConsumerConcurrency()).thenReturn(1);
    when(opts.getTopicDefaultPartitionCount()).thenReturn(3);
    return opts;
  }

  private PostgresSqlSetupProperties mockPostgresProps(PgQueueSetupOptions opts) {
    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    when(props.buildPgQueueOptions(any())).thenReturn(opts);
    return props;
  }

  private ConfigurationProvider mockConfigProvider() {
    ConfigurationProvider cp = mock(ConfigurationProvider.class);
    when(cp.getKafka()).thenReturn(mock(KafkaConfiguration.class));
    return cp;
  }

  private QueueReceivedMessage stubMessage() {
    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.now(), 1L, 0, 1L);
    return new QueueReceivedMessage(
        handle,
        5,
        new byte[] {1, 2, 3, 4},
        Optional.empty(),
        PgQueuePayloadCompression.NONE,
        List.of(),
        "key",
        "owner");
  }

  private void runWorkerAndJoin(PgQueuePollWorker worker, long timeoutMs)
      throws InterruptedException {
    Thread t = new Thread(worker);
    t.start();
    t.join(timeoutMs);
    if (t.isAlive()) {
      worker.stop();
      t.interrupt();
      t.join(500);
    }
  }

  // --- 1. Constructor validation ---

  @Test
  public void constructorRejectsShardCountLessThanOne() {
    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP, List.of(TOPIC), 10, "thread-0", 100, 100, 100, (topic, msgs, ctx) -> {});
    try {
      new PgQueuePollWorker(
          reg,
          mock(MetadataQueueStore.class),
          mock(PostgresSqlSetupProperties.class),
          mock(ConfigurationProvider.class),
          mockSetupOptions(),
          0,
          0,
          null);
      fail("Expected IllegalArgumentException for workerShardCount < 1");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("workerShardCount must be >= 1"));
    }
  }

  @Test
  public void constructorRejectsShardIndexOutOfRange() {
    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP, List.of(TOPIC), 10, "thread-0", 100, 100, 100, (topic, msgs, ctx) -> {});
    try {
      new PgQueuePollWorker(
          reg,
          mock(MetadataQueueStore.class),
          mock(PostgresSqlSetupProperties.class),
          mock(ConfigurationProvider.class),
          mockSetupOptions(),
          3,
          2,
          null);
      fail("Expected IllegalArgumentException for workerShardIndex >= workerShardCount");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("workerShardIndex must be in"));
    }

    try {
      new PgQueuePollWorker(
          reg,
          mock(MetadataQueueStore.class),
          mock(PostgresSqlSetupProperties.class),
          mock(ConfigurationProvider.class),
          mockSetupOptions(),
          -1,
          2,
          null);
      fail("Expected IllegalArgumentException for negative workerShardIndex");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("workerShardIndex must be in"));
    }
  }

  // --- 2. Immediate mode dispatch ---

  @Test
  public void immediateModeDispatchesHandlerAndStops() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));

    QueueReceivedMessage msg = stubMessage();
    PgQueuePollHandler handler = mock(PgQueuePollHandler.class);

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP, List.of(TOPIC), 10, "thread-0", 100, 100, 100, handler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            (Answer<List<QueueReceivedMessage>>)
                inv -> {
                  worker.stop();
                  return List.of(msg);
                });

    runWorkerAndJoin(worker, 2000);

    verify(handler).handleBatch(eq(TOPIC), eq(List.of(msg)), any(PgQueuePollContext.class));
    assertTrue(worker.isStopped());
  }

  // --- 3. Handler exception is swallowed ---

  @Test
  public void immediateModeSwallowsHandlerException() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));

    QueueReceivedMessage msg = stubMessage();
    AtomicInteger handlerCalls = new AtomicInteger(0);

    PgQueuePollHandler handler =
        (topic, msgs, ctx) -> {
          int call = handlerCalls.incrementAndGet();
          if (call == 1) {
            throw new RuntimeException("boom");
          }
        };

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(GROUP, List.of(TOPIC), 10, "thread-0", 50, 100, 100, handler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    AtomicInteger receiveCalls = new AtomicInteger(0);
    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            (Answer<List<QueueReceivedMessage>>)
                inv -> {
                  if (receiveCalls.incrementAndGet() >= 2) {
                    worker.stop();
                  }
                  return List.of(msg);
                });

    runWorkerAndJoin(worker, 2000);

    assertEquals(handlerCalls.get(), 2);
    assertTrue(worker.isStopped());
  }

  // --- 4. Missing topic sleeps ---

  @Test
  public void immediateModeSleepsWhenTopicMissing() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);

    AtomicInteger fetchCalls = new AtomicInteger(0);
    when(store.fetchTopic(TOPIC))
        .thenAnswer(
            inv -> {
              if (fetchCalls.incrementAndGet() >= 2) {
                Thread.currentThread().interrupt();
              }
              return Optional.empty();
            });

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP, List.of(TOPIC), 10, "thread-0", 100, 10, 100, (topic, msgs, ctx) -> {});

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    runWorkerAndJoin(worker, 2000);

    assertTrue(
        fetchCalls.get() >= 2, "Should have polled fetchTopic multiple times before interrupt");
    verify(store, never())
        .receiveBatchForGroup(anyString(), anyLong(), anyList(), anyString(), any(), anyInt());
  }

  // --- 5. Empty poll sleeps ---

  @Test
  public void immediateModeSleepsOnEmptyBatch() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));

    PgQueuePollHandler handler = mock(PgQueuePollHandler.class);
    AtomicInteger receiveCalls = new AtomicInteger(0);

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(GROUP, List.of(TOPIC), 10, "thread-0", 10, 100, 100, handler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            (Answer<List<QueueReceivedMessage>>)
                inv -> {
                  if (receiveCalls.incrementAndGet() >= 2) {
                    worker.stop();
                  }
                  return List.of();
                });

    runWorkerAndJoin(worker, 2000);

    assertTrue(receiveCalls.get() >= 2, "Should have polled receiveBatch multiple times");
    verify(handler, never()).handleBatch(anyString(), anyList(), any());
  }

  // --- 6. Accumulation mode: messages buffered and flushed on threshold ---

  @Test
  public void accumulationModeFlushesOnMessageCountThreshold() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));

    PgQueueBatchFlushHandler flushHandler = mock(PgQueueBatchFlushHandler.class);
    PgQueueBatchPolicy policy = new PgQueueBatchPolicy(2, Long.MAX_VALUE, 60_000);
    QueueReceivedMessage msg1 = stubMessage();
    QueueReceivedMessage msg2 = stubMessage();

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP,
            List.of(TOPIC),
            10,
            "thread-0",
            10,
            100,
            100,
            (topic, msgs, ctx) -> {},
            policy,
            flushHandler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    AtomicInteger receiveCalls = new AtomicInteger(0);
    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            (Answer<List<QueueReceivedMessage>>)
                inv -> {
                  int call = receiveCalls.incrementAndGet();
                  if (call == 1) {
                    return List.of(msg1);
                  } else if (call == 2) {
                    return List.of(msg2);
                  } else {
                    worker.stop();
                    return List.of();
                  }
                });

    runWorkerAndJoin(worker, 2000);

    verify(flushHandler).flush(eq(TOPIC), argThat(batch -> batch.size() == 2), any());
  }

  // --- 7. Accumulation mode: linger timeout flush on empty poll ---

  @Test
  public void accumulationModeFlushesOnLingerTimeout() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));

    PgQueueBatchFlushHandler flushHandler = mock(PgQueueBatchFlushHandler.class);
    // Linger flush runs on an empty poll after maxAgeMs; use a small but reliable window for CI.
    PgQueueBatchPolicy policy = new PgQueueBatchPolicy(100, Long.MAX_VALUE, 10);

    QueueReceivedMessage msg = stubMessage();

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP,
            List.of(TOPIC),
            10,
            "thread-0",
            10,
            50,
            100,
            (topic, msgs, ctx) -> {},
            policy,
            flushHandler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    AtomicInteger receiveCalls = new AtomicInteger(0);
    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            (Answer<List<QueueReceivedMessage>>)
                inv -> {
                  int call = receiveCalls.incrementAndGet();
                  if (call == 1) {
                    return List.of(msg);
                  }
                  // Wait for linger expiry before the empty poll that triggers flush.
                  Thread.sleep(20);
                  if (call >= 3) {
                    worker.stop();
                  }
                  return List.of();
                });

    runWorkerAndJoin(worker, 5000);

    verify(flushHandler, timeout(3000))
        .flush(eq(TOPIC), argThat(batch -> batch.size() == 1), any());
  }

  // --- 8. Error recovery sleep ---

  @Test
  public void errorRecoverySleepOnStoreException() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));

    PgQueuePollHandler handler = mock(PgQueuePollHandler.class);
    AtomicInteger receiveCalls = new AtomicInteger(0);

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(GROUP, List.of(TOPIC), 10, "thread-0", 100, 100, 10, handler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            (Answer<List<QueueReceivedMessage>>)
                inv -> {
                  int call = receiveCalls.incrementAndGet();
                  if (call == 1) {
                    throw new RuntimeException("db down");
                  }
                  worker.stop();
                  return List.of();
                });

    runWorkerAndJoin(worker, 2000);

    assertTrue(receiveCalls.get() >= 2, "Worker should have retried after error recovery sleep");
    assertFalse(Thread.currentThread().isInterrupted());
    verify(handler, never()).handleBatch(anyString(), anyList(), any());
  }

  @Test
  public void immediateModeWarnStuckAheadDoesNotThrow() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));
    when(store.detectOffsetAheadOfLog(eq(GROUP), eq(1L), eq(3)))
        .thenReturn(
            List.of(
                PartitionOffsetSkew.builder()
                    .consumerGroup(GROUP)
                    .topicId(1L)
                    .partitionId(0)
                    .committedOffset(10L)
                    .maxSeq(5L)
                    .aheadBy(5L)
                    .build()));

    PgQueuePollHandler handler = mock(PgQueuePollHandler.class);
    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP, List.of(TOPIC), 10, "thread-0", 100, 100, 100, handler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              worker.stop();
              return List.of();
            });

    runWorkerAndJoin(worker, 2000);

    verify(store).detectOffsetAheadOfLog(GROUP, 1L, 3);
    verify(handler, never()).handleBatch(anyString(), anyList(), any());
  }

  @Test
  public void immediateModeSwallowsUnsupportedOffsetSkewCheck() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));
    when(store.detectOffsetAheadOfLog(anyString(), anyLong(), anyInt()))
        .thenThrow(new UnsupportedOperationException("not pgQueue store"));

    PgQueuePollHandler handler = mock(PgQueuePollHandler.class);
    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP, List.of(TOPIC), 10, "thread-0", 100, 100, 100, handler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              worker.stop();
              return List.of();
            });

    runWorkerAndJoin(worker, 2000);

    verify(handler, never()).handleBatch(anyString(), anyList(), any());
  }

  @Test
  public void accumulationModeFlushesOnByteThresholdDuringPoll() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));

    PgQueueBatchFlushHandler flushHandler = mock(PgQueueBatchFlushHandler.class);
    PgQueueBatchPolicy policy = new PgQueueBatchPolicy(100, 7, 60_000);
    QueueReceivedMessage msg1 = stubMessage();
    QueueReceivedMessage msg2 = stubMessage();

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP,
            List.of(TOPIC),
            10,
            "thread-0",
            10,
            100,
            100,
            (topic, msgs, ctx) -> {},
            policy,
            flushHandler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    AtomicInteger receiveCalls = new AtomicInteger(0);
    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              int call = receiveCalls.incrementAndGet();
              if (call == 1) {
                return List.of(msg1, msg2);
              }
              worker.stop();
              return List.of();
            });

    runWorkerAndJoin(worker, 2000);

    verify(flushHandler).flush(eq(TOPIC), argThat(batch -> batch.size() == 2), any());
  }

  @Test
  public void accumulationModeSwallowsFlushHandlerException() throws Exception {
    PgQueueSetupOptions opts = mockSetupOptions();
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.of(TOPIC_META));

    PgQueueBatchFlushHandler flushHandler = mock(PgQueueBatchFlushHandler.class);
    doThrow(new RuntimeException("flush failed"))
        .when(flushHandler)
        .flush(anyString(), anyList(), any());

    PgQueueBatchPolicy policy = new PgQueueBatchPolicy(1, Long.MAX_VALUE, 60_000);
    QueueReceivedMessage msg = stubMessage();

    PgQueuePollerRegistration reg =
        new PgQueuePollerRegistration(
            GROUP,
            List.of(TOPIC),
            10,
            "thread-0",
            10,
            100,
            100,
            (topic, msgs, ctx) -> {},
            policy,
            flushHandler);

    PgQueuePollWorker worker =
        new PgQueuePollWorker(
            reg, store, mockPostgresProps(opts), mockConfigProvider(), opts, 0, 1, null);

    when(store.receiveBatchForGroup(
            anyString(), anyLong(), anyList(), anyString(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              worker.stop();
              return List.of(msg);
            });

    runWorkerAndJoin(worker, 2000);

    verify(flushHandler).flush(eq(TOPIC), anyList(), any());
    assertTrue(worker.isStopped());
  }
}
