package com.linkedin.gms.factory.auth;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PolicyChangeListenerTest {

  private static final String TOPIC = "PolicyChange_v1";
  private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);
  private static final PolicyChangeListener.Settings SETTINGS =
      new PolicyChangeListener.Settings(
          Duration.ZERO, Duration.ZERO, Duration.ofMinutes(5), Duration.ofSeconds(5));

  private MockConsumer<byte[], byte[]> consumer;
  private AtomicInteger loads;
  private MutableClock clock;
  private PolicyChangeListener listener;

  /** A clock the tests can move forward. */
  private static final class MutableClock extends Clock {
    long millis = 1_000_000L;

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(millis);
    }
  }

  @BeforeMethod
  public void setup() {
    consumer = new MockConsumer<>("none");
    loads = new AtomicInteger();
    clock = new MutableClock();
    listener =
        new PolicyChangeListener(
            consumer,
            TOPIC,
            () -> {
              loads.incrementAndGet();
              return CompletableFuture.completedFuture(null);
            },
            null,
            SETTINGS,
            clock);
  }

  private void topicWithPartitions(TopicPartition... partitions) {
    List<PartitionInfo> infos = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      infos.add(new PartitionInfo(TOPIC, tp.partition(), Node.noNode(), null, null));
    }
    consumer.updatePartitions(TOPIC, infos);
  }

  private void record(TopicPartition tp, long offset) {
    consumer.addRecord(
        new ConsumerRecord<>(TOPIC, tp.partition(), offset, new byte[0], new byte[0]));
  }

  @Test
  public void testStartLoadsOnceAndReadsFromTheEndOffsets() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 7L));

    assertTrue(listener.step());

    assertEquals(loads.get(), 1);
    assertEquals(consumer.position(P0), 7L);
  }

  @Test
  public void testEachPolledBatchRequestsOneLoad() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 0L));
    listener.step();
    loads.set(0);

    record(P0, 0);
    record(P0, 1);
    listener.step();
    assertEquals(loads.get(), 1, "two messages in one poll share one load");

    listener.step();
    assertEquals(loads.get(), 1, "an empty poll does not load");

    record(P0, 2);
    listener.step();
    assertEquals(loads.get(), 2);
  }

  @Test
  public void testAllPartitionsAreRead() throws Exception {
    topicWithPartitions(P0, P1);
    consumer.updateEndOffsets(Map.of(P0, 3L, P1, 5L));

    listener.step();

    assertEquals(consumer.assignment(), Set.of(P0, P1));
    assertEquals(consumer.position(P0), 3L);
    assertEquals(consumer.position(P1), 5L);
  }

  @Test
  public void testMissingTopicWaitsWithoutLoading() throws Exception {
    assertFalse(listener.step());
    assertEquals(loads.get(), 0);

    // The topic is created later; the next step starts reading.
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 0L));
    assertTrue(listener.step());
    assertEquals(loads.get(), 1);
  }

  @Test
  public void testLostPositionLoadsAgainAndReadsFromTheNewEnd() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 10L));
    listener.step();

    // For example, the topic was deleted and created again: the old offset no longer exists.
    consumer.setPollException(new OffsetOutOfRangeException(Map.of(P0, 10L)));
    consumer.updateEndOffsets(Map.of(P0, 2L));
    assertTrue(listener.step());
    assertTrue(listener.step());

    assertEquals(loads.get(), 2);
    assertEquals(consumer.position(P0), 2L);
  }

  @Test
  public void testTopicRemovedLaterIsFoundOnTheNextCheck() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 0L));
    listener.step();

    consumer.updatePartitions(TOPIC, List.of());
    assertTrue(listener.step(), "the topic is not checked again before the interval");

    clock.millis += Duration.ofMinutes(5).toMillis();
    assertFalse(listener.step());
    assertEquals(loads.get(), 1);
  }

  @Test
  public void testAddedPartitionIsAssignedOnTheNextCheck() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 3L));
    listener.step();

    // An operator adds a partition. Messages keyed to it are read only after the next check.
    topicWithPartitions(P0, P1);
    consumer.updateEndOffsets(Map.of(P0, 3L, P1, 0L));
    clock.millis += Duration.ofMinutes(5).toMillis();
    assertTrue(listener.step());

    assertEquals(consumer.assignment(), Set.of(P0, P1));
    assertEquals(
        loads.get(), 2, "positioning again loads, so nothing on the new partition is lost");
    record(P1, 0);
    listener.step();
    assertEquals(loads.get(), 3);
  }

  @Test
  public void testFailedStartLoadDoesNotSeek() {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 4L));
    PolicyChangeListener failing =
        new PolicyChangeListener(
            consumer,
            TOPIC,
            () -> CompletableFuture.failedFuture(new IllegalStateException("database down")),
            null,
            SETTINGS,
            clock);

    // The run loop logs this and starts again after the retry interval.
    expectThrows(Exception.class, failing::step);
  }

  @Test
  public void testTopicCheckTimeoutKeepsThePosition() throws Exception {
    final boolean[] slowBroker = {false};
    MockConsumer<byte[], byte[]> slow =
        new MockConsumer<>("none") {
          @Override
          public synchronized List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
            if (slowBroker[0]) {
              throw new TimeoutException("simulated metadata timeout");
            }
            return super.partitionsFor(topic, timeout);
          }
        };
    slow.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, Node.noNode(), null, null)));
    slow.updateEndOffsets(Map.of(P0, 3L));
    PolicyChangeListener timed =
        new PolicyChangeListener(
            slow,
            TOPIC,
            () -> {
              loads.incrementAndGet();
              return CompletableFuture.completedFuture(null);
            },
            null,
            SETTINGS,
            clock);
    timed.step();
    assertEquals(loads.get(), 1);

    slowBroker[0] = true;
    clock.millis += Duration.ofMinutes(5).toMillis();
    // A timeout is not a change: no exception, no new load, same assignment.
    assertTrue(timed.step());
    assertEquals(loads.get(), 1);
    assertEquals(slow.assignment(), Set.of(P0));
  }

  @Test
  public void testListenerPropertiesReadWithoutAGroup() {
    Map<String, Object> props =
        PolicyChangeListenerFactory.listenerProperties(
            Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "generic-mae-consumer-job-client",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092"));

    assertFalse(props.containsKey(ConsumerConfig.GROUP_ID_CONFIG));
    assertEquals(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), false);
    assertEquals(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "none");
    assertEquals(props.get(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG), false);
    assertEquals(
        props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), ByteArrayDeserializer.class);
    assertEquals(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), "broker:9092");
    assertEquals(props.get(ConsumerConfig.METADATA_MAX_AGE_CONFIG), 30_000);
  }
}
