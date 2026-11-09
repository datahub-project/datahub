package com.datahub.metadata.dao.throttle;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.mxe.Topics;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

/** {@code close()} on both sensors stops the lag refresh, which Spring relies on at shutdown. */
public class ThrottleSensorCloseTest {

  private static final int REFRESH_INTERVAL_MS = 10;
  private static final String VERSIONED_TOPIC = Topics.METADATA_CHANGE_LOG_VERSIONED;
  private static final String TIMESERIES_TOPIC = Topics.METADATA_CHANGE_LOG_TIMESERIES;
  private static final String CONSUMER_GROUP = "generic-mae-consumer-job-client";

  @Test
  public void testKafkaSensorCloseStopsRefreshAndClosesAdminClient() throws Exception {
    AdminClient admin = caughtUpAdmin();
    KafkaThrottleSensor sensor = kafkaSensor(admin).start();
    verify(admin, timeout(5_000).atLeastOnce()).listConsumerGroupOffsets(anyString());

    sensor.close();

    verify(admin).close(any(Duration.class));
    assertNoMoreCalls(admin);
  }

  @Test
  public void testKafkaSensorCloseInterruptsAndWaitsForARefreshWaitingOnKafka() throws Exception {
    CountDownLatch fetching = new CountDownLatch(1);
    AtomicBoolean refreshFinished = new AtomicBoolean(false);
    AdminClient admin = mock(AdminClient.class);
    KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> hangingFuture = mock(KafkaFuture.class);
    when(hangingFuture.get())
        .thenAnswer(
            invocation -> {
              fetching.countDown();
              try {
                Thread.sleep(Long.MAX_VALUE);
              } catch (InterruptedException e) {
                // a refresh that takes a moment to unwind after the interrupt
                Thread.sleep(200);
                refreshFinished.set(true);
                throw e;
              }
              return Map.of();
            });
    ListConsumerGroupOffsetsResult committed = mock(ListConsumerGroupOffsetsResult.class);
    when(committed.partitionsToOffsetAndMetadata()).thenReturn(hangingFuture);
    when(admin.listConsumerGroupOffsets(anyString())).thenReturn(committed);

    KafkaThrottleSensor sensor = kafkaSensor(admin).start();
    assertTrue(fetching.await(5, TimeUnit.SECONDS), "Expected a refresh waiting on Kafka");

    long startNanos = System.nanoTime();
    sensor.close();

    // close() interrupted the fetch instead of waiting out its 5s shutdown timeout ...
    assertTrue(
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos) < 4_000,
        "Expected close() to interrupt the refresh");
    // ... and let the refresh finish before closing the admin client
    assertTrue(refreshFinished.get(), "Expected close() to wait for the refresh to finish");
    verify(admin).close(any(Duration.class));
  }

  @Test
  public void testPgQueueSensorCloseStopsRefresh() throws Exception {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(anyString())).thenReturn(Optional.empty());
    PgQueueThrottleSensor sensor = pgQueueSensor(store).start();
    verify(store, timeout(5_000).atLeastOnce()).fetchTopic(anyString());

    sensor.close();

    assertNoMoreCalls(store);
  }

  @Test
  public void testCloseGivesUpOnARefreshThatIgnoresInterrupts() throws Exception {
    StuckRefresh stuck = new StuckRefresh();
    AdminClient admin = stuckAdmin(stuck);
    MetadataQueueStore store = stuckStore(stuck);
    KafkaThrottleSensor kafkaSensor =
        kafkaSensor(admin).toBuilder().shutdownTimeout(Duration.ofMillis(100)).build().start();
    PgQueueThrottleSensor pgQueueSensor =
        pgQueueSensor(store).toBuilder().shutdownTimeout(Duration.ofMillis(100)).build().start();
    try {
      assertTrue(stuck.entered.await(5, TimeUnit.SECONDS), "Expected refreshes in flight");

      long startNanos = System.nanoTime();
      kafkaSensor.close();
      pgQueueSensor.close();

      assertTrue(
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos) < 4_000,
          "Expected close() to stop waiting after its shutdown timeout");
      verify(admin).close(Duration.ofMillis(100));
    } finally {
      stuck.release.set(true);
    }
  }

  @Test
  public void testCloseFromAnInterruptedThreadKeepsTheInterrupt() throws Exception {
    StuckRefresh stuck = new StuckRefresh();
    AdminClient admin = stuckAdmin(stuck);
    MetadataQueueStore store = stuckStore(stuck);
    KafkaThrottleSensor kafkaSensor = kafkaSensor(admin).start();
    PgQueueThrottleSensor pgQueueSensor = pgQueueSensor(store).start();
    try {
      assertTrue(stuck.entered.await(5, TimeUnit.SECONDS), "Expected refreshes in flight");

      Thread.currentThread().interrupt();
      kafkaSensor.close();
      assertTrue(Thread.currentThread().isInterrupted(), "close() must keep the interrupt");
      pgQueueSensor.close();
      assertTrue(Thread.interrupted(), "close() must keep the interrupt");

      verify(admin).close(any(Duration.class));
    } finally {
      Thread.interrupted();
      stuck.release.set(true);
    }
  }

  /** A refresh that ignores interrupts and only returns once released. */
  private static final class StuckRefresh {
    final CountDownLatch entered = new CountDownLatch(2);
    final AtomicBoolean release = new AtomicBoolean(false);

    void block() {
      entered.countDown();
      while (!release.get()) {
        Thread.onSpinWait();
      }
    }
  }

  private static AdminClient stuckAdmin(StuckRefresh stuck) {
    AdminClient admin = mock(AdminClient.class);
    when(admin.listConsumerGroupOffsets(anyString()))
        .thenAnswer(
            invocation -> {
              stuck.block();
              throw new IllegalStateException("released");
            });
    return admin;
  }

  private static MetadataQueueStore stuckStore(StuckRefresh stuck) {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(anyString()))
        .thenAnswer(
            invocation -> {
              stuck.block();
              return Optional.empty();
            });
    return store;
  }

  private static PgQueueThrottleSensor pgQueueSensor(MetadataQueueStore store) {
    return PgQueueThrottleSensor.builder()
        .metadataQueueStore(store)
        .config(schedulerConfig())
        .mclConsumerGroupId(CONSUMER_GROUP)
        .versionedTopicName(VERSIONED_TOPIC)
        .timeseriesTopicName(TIMESERIES_TOPIC)
        .build();
  }

  /** close() waits for a refresh in flight, so the mock must see no calls after it returns. */
  private static void assertNoMoreCalls(Object mock) throws InterruptedException {
    int callsAtClose = mockingDetails(mock).getInvocations().size();
    Thread.sleep(REFRESH_INTERVAL_MS * 20);
    assertEquals(mockingDetails(mock).getInvocations().size(), callsAtClose);
  }

  private static KafkaThrottleSensor kafkaSensor(AdminClient admin) {
    return KafkaThrottleSensor.builder()
        .config(schedulerConfig())
        .kafkaAdmin(admin)
        .versionedTopicName(VERSIONED_TOPIC)
        .timeseriesTopicName(TIMESERIES_TOPIC)
        .entityRegistry(mock(EntityRegistry.class))
        .mclConsumerGroupId(CONSUMER_GROUP)
        .build();
  }

  /** Refreshes every 10ms; the threshold is above any lag here, so throttle() never sleeps. */
  private static MetadataChangeProposalConfig.ThrottlesConfig schedulerConfig() {
    MetadataChangeProposalConfig.ThrottlesConfig config =
        new MetadataChangeProposalConfig.ThrottlesConfig().setUpdateIntervalMs(REFRESH_INTERVAL_MS);
    config.setVersioned(
        new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(true).setThreshold(1000));
    config.setTimeseries(new MetadataChangeProposalConfig.ThrottleConfig().setEnabled(false));
    return config;
  }

  /** An admin client where the consumer group is caught up on both MCL topics. */
  private static AdminClient caughtUpAdmin() {
    TopicPartition versioned = new TopicPartition(VERSIONED_TOPIC, 0);
    TopicPartition timeseries = new TopicPartition(TIMESERIES_TOPIC, 0);
    AdminClient admin = mock(AdminClient.class);
    ListConsumerGroupOffsetsResult committed = mock(ListConsumerGroupOffsetsResult.class);
    when(committed.partitionsToOffsetAndMetadata())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    versioned, new OffsetAndMetadata(10), timeseries, new OffsetAndMetadata(20))));
    when(admin.listConsumerGroupOffsets(anyString())).thenReturn(committed);
    ListOffsetsResult endOffsets = mock(ListOffsetsResult.class);
    when(endOffsets.all())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    versioned, new ListOffsetsResultInfo(10, 0, Optional.empty()),
                    timeseries, new ListOffsetsResultInfo(20, 0, Optional.empty()))));
    when(admin.listOffsets(anyMap())).thenReturn(endOffsets);
    return admin;
  }
}
