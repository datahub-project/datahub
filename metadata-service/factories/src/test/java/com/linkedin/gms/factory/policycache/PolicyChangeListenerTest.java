package com.linkedin.gms.factory.policycache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
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
          Duration.ZERO,
          Duration.ZERO,
          Duration.ofMinutes(5),
          Duration.ofSeconds(5),
          Duration.ofMinutes(10));

  private ReplayConsumer consumer;
  private AtomicInteger loads;
  private List<Set<Urn>> updates;
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

  /** MockConsumer does not implement offsetsForTimes; this one returns set replay offsets. */
  private static class ReplayConsumer extends MockConsumer<byte[], byte[]> {
    final Map<TopicPartition, Long> replayOffsets = new HashMap<>();

    ReplayConsumer() {
      super("none");
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch) {
      Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
      for (TopicPartition tp : timestampsToSearch.keySet()) {
        Long offset = replayOffsets.get(tp);
        result.put(tp, offset == null ? null : new OffsetAndTimestamp(offset, 0L));
      }
      return result;
    }
  }

  private PolicyChangeListener listener(ReplayConsumer kafka) {
    return new PolicyChangeListener(
        kafka,
        TOPIC,
        () -> {
          loads.incrementAndGet();
          return CompletableFuture.completedFuture(null);
        },
        urns -> {
          updates.add(urns);
          return CompletableFuture.completedFuture(null);
        },
        null,
        SETTINGS,
        clock);
  }

  @BeforeMethod
  public void setup() {
    consumer = new ReplayConsumer();
    loads = new AtomicInteger();
    updates = new CopyOnWriteArrayList<>();
    clock = new MutableClock();
    listener = listener(consumer);
  }

  private static Urn urn(String id) {
    return UrnUtils.getUrn("urn:li:dataHubPolicy:" + id);
  }

  private void topicWithPartitions(TopicPartition... partitions) {
    List<PartitionInfo> infos = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      infos.add(new PartitionInfo(TOPIC, tp.partition(), Node.noNode(), null, null));
    }
    consumer.updatePartitions(TOPIC, infos);
  }

  private void record(TopicPartition tp, long offset, String key) {
    consumer.addRecord(
        new ConsumerRecord<>(
            TOPIC, tp.partition(), offset, key.getBytes(StandardCharsets.UTF_8), new byte[0]));
  }

  private void record(TopicPartition tp, long offset) {
    record(tp, offset, urn("p" + offset).toString());
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
  public void testEachPolledBatchRequestsOneUpdateOfItsPolicies() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 0L));
    listener.step();
    loads.set(0);

    record(P0, 0);
    record(P0, 1);
    listener.step();
    assertEquals(updates, List.of(Set.of(urn("p0"), urn("p1"))), "one poll, one key read");

    listener.step();
    assertEquals(updates.size(), 1, "an empty poll reads nothing");

    record(P0, 2);
    listener.step();
    assertEquals(updates.get(1), Set.of(urn("p2")));
    assertEquals(loads.get(), 0, "messages never cause a full load");
  }

  @Test
  public void testStartReplaysTheWindowByKey() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 7L));
    // Messages 4 to 6 are within the replay window. The full load may have missed them.
    consumer.replayOffsets.put(P0, 4L);

    listener.step();
    assertEquals(loads.get(), 1);
    assertEquals(consumer.position(P0), 4L);

    record(P0, 4);
    record(P0, 5);
    record(P0, 6);
    listener.step();
    assertEquals(updates, List.of(Set.of(urn("p4"), urn("p5"), urn("p6"))));
  }

  @Test
  public void testUnreadableKeysRequestAFullLoad() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 0L));
    listener.step();

    record(P0, 0, "not a urn");
    // A URN, but not a policy.
    record(P0, 1, "urn:li:corpuser:someone");
    consumer.addRecord(new ConsumerRecord<>(TOPIC, P0.partition(), 2L, null, new byte[0]));
    listener.step();

    assertEquals(loads.get(), 2);
    assertEquals(updates, List.of());
  }

  @Test
  public void testMixedBatchStillReadsTheGoodKeys() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 0L));
    listener.step();

    record(P0, 0);
    record(P0, 1, "not a urn");
    listener.step();

    // Both requests: they usually merge into one full load that also reads p0 by key, so p0 goes on
    // the
    // recent-changes list.
    assertEquals(loads.get(), 2);
    assertEquals(updates, List.of(Set.of(urn("p0"))));
  }

  @Test
  public void testStartAfterCloseDoesNothing() {
    listener.close();
    listener.start();
    assertEquals(loads.get(), 0);
  }

  @Test
  public void testCloseDoesNotWaitForASlowStartLoad() throws Exception {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 0L));
    CompletableFuture<Void> neverDone = new CompletableFuture<>();
    CompletableFuture<Void> requested = new CompletableFuture<>();
    PolicyChangeListener slow =
        new PolicyChangeListener(
            consumer,
            TOPIC,
            () -> {
              requested.complete(null);
              return neverDone;
            },
            urns -> CompletableFuture.completedFuture(null),
            null,
            new PolicyChangeListener.Settings(
                Duration.ofMillis(100),
                Duration.ofMillis(100),
                Duration.ofMinutes(5),
                Duration.ofSeconds(90),
                Duration.ofMinutes(10)),
            clock);
    slow.start();
    requested.get(10, TimeUnit.SECONDS);

    long start = System.nanoTime();
    slow.close();
    long closeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    // The start load waits in 1 s steps, so close() returns long before the 90 s load timeout.
    assertTrue(closeMillis < 5_000, "close took " + closeMillis + " ms");
    assertTrue(consumer.closed(), "the listener closed its consumer");
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
    assertEquals(updates, List.of(Set.of(urn("p0"))));
  }

  @Test
  public void testFailedStartLoadDoesNotSeek() {
    topicWithPartitions(P0);
    consumer.updateEndOffsets(Map.of(P0, 4L));
    PolicyChangeListener failing =
        new PolicyChangeListener(
            consumer,
            TOPIC,
            () -> CompletableFuture.failedFuture(new IllegalStateException("search down")),
            urns -> CompletableFuture.completedFuture(null),
            null,
            SETTINGS,
            clock);

    // The run loop logs this and starts again after the retry interval.
    expectThrows(Exception.class, failing::step);
    // No seek to the end offset: the next start loads again before it reads, so no change is lost.
    assertEquals(consumer.assignment(), Set.of(P0));
    expectThrows(NoOffsetForPartitionException.class, () -> consumer.position(P0));
  }

  @Test
  public void testTopicCheckTimeoutKeepsThePosition() throws Exception {
    final boolean[] slowBroker = {false};
    ReplayConsumer slow =
        new ReplayConsumer() {
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
    PolicyChangeListener timed = listener(slow);
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
