package com.linkedin.gms.factory.policycache;

import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Reads the policy change topic in every GMS, and reads each changed policy into the policy cache
 * when a policy changes anywhere.
 *
 * <p>The listener needs no consumer group and commits no offsets: each GMS reads every message. The
 * message key is the policy URN; the value is not used. A message whose key is not a URN requests a
 * full load instead.
 *
 * <p>How it starts reading, without a gap:
 *
 * <ol>
 *   <li>assign all partitions of the topic;
 *   <li>run a full load. It lists policies from the search index, which can miss a change made
 *       shortly before;
 *   <li>seek back by the replay window (the recent-changes window, 10 minutes by default) and read
 *       the policies of those messages by key. So a change the full load missed is still applied,
 *       and every later change is read too.
 * </ol>
 *
 * <p>Repairs: when the position is lost (for example the topic was recreated), when the topic is
 * missing or its partitions change (checked every 30 s, from client metadata that is at most 30 s
 * old), or on any other error, the listener starts again from step 1. While it cannot read the
 * topic, the cache still refreshes on its timer.
 */
@Slf4j
public class PolicyChangeListener implements AutoCloseable {

  /** Tuning values for the listener. */
  public record Settings(
      @Nonnull Duration pollTimeout,
      @Nonnull Duration retryInterval,
      @Nonnull Duration topicCheckInterval,
      @Nonnull Duration loadTimeout,
      @Nonnull Duration replayWindow) {
    public static Settings defaults(@Nonnull Duration replayWindow) {
      return new Settings(
          Duration.ofSeconds(1),
          Duration.ofSeconds(30),
          Duration.ofSeconds(30),
          Duration.ofSeconds(90),
          replayWindow);
    }
  }

  private static final Duration TOPIC_CHECK_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration LOAD_WAIT_STEP = Duration.ofSeconds(1);

  private final Consumer<byte[], byte[]> consumer;
  private final String topic;
  private final Supplier<CompletableFuture<Void>> loadRequester;
  private final Function<Set<Urn>, CompletableFuture<Void>> updateRequester;
  @Nullable private final MetricUtils metricUtils;
  private final Settings settings;
  private final Clock clock;

  private final CountDownLatch stopped = new CountDownLatch(1);
  private volatile boolean running;
  @Nullable private Thread thread;

  // Only read and written on the listener thread.
  private boolean positioned;
  private boolean positionedBefore;
  private boolean missingTopicLogged;
  private long nextTopicCheckMillis;
  // The end offsets when the listener last positioned: messages below them are replays.
  private Map<TopicPartition, Long> replayEnds = Map.of();

  public PolicyChangeListener(
      @Nonnull Consumer<byte[], byte[]> consumer,
      @Nonnull String topic,
      @Nonnull Supplier<CompletableFuture<Void>> loadRequester,
      @Nonnull Function<Set<Urn>, CompletableFuture<Void>> updateRequester,
      @Nullable MetricUtils metricUtils,
      @Nonnull Settings settings,
      @Nonnull Clock clock) {
    this.consumer = consumer;
    this.topic = topic;
    this.loadRequester = loadRequester;
    this.updateRequester = updateRequester;
    this.metricUtils = metricUtils;
    this.settings = settings;
    this.clock = clock;
  }

  /** Starts the listener thread. The process does not wait for it. */
  public synchronized void start() {
    // Not again after close(): the stopped latch is already released.
    if (thread != null || stopped.getCount() == 0) {
      return;
    }
    running = true;
    thread = new Thread(this::runLoop, "policy-change-listener");
    thread.setDaemon(true);
    thread.start();
    log.info("policy change listener started, topic={}", topic);
  }

  @Override
  public void close() {
    final Thread t;
    synchronized (this) {
      running = false;
      stopped.countDown();
      t = thread;
    }
    if (t == null) {
      consumer.close();
      return;
    }
    consumer.wakeup();
    try {
      t.join(TimeUnit.SECONDS.toMillis(10));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void runLoop() {
    try {
      while (running) {
        try {
          if (!step()) {
            pause();
          }
        } catch (WakeupException e) {
          if (!running) {
            break;
          }
        } catch (InterruptException e) {
          if (!running) {
            break;
          }
          // The Kafka client sets the interrupt flag again, so every later poll would throw at
          // once.
          // close() stops the listener with wakeup(), not an interrupt: clear it and start again.
          Thread.interrupted();
          log.warn(
              "policy change listener was interrupted; starts again in {}",
              settings.retryInterval());
          positioned = false;
          pause();
        } catch (Exception e) {
          log.warn(
              "policy change listener failed; starts again in {}", settings.retryInterval(), e);
          positioned = false;
          pause();
        }
      }
    } finally {
      // An interrupt that ended the loop would make close() throw; clear it, close, then restore
      // it.
      final boolean interrupted = Thread.interrupted();
      try {
        consumer.close();
      } catch (RuntimeException e) {
        log.warn("policy change listener could not close its consumer", e);
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      log.info("policy change listener stopped");
    }
  }

  private void pause() {
    try {
      stopped.await(settings.retryInterval().toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      running = false;
    }
  }

  /**
   * One unit of work: position the consumer if needed, then poll once. Returns false when the
   * listener cannot read now and should wait before the next step.
   */
  boolean step() throws Exception {
    if (positioned && clock.millis() >= nextTopicCheckMillis && !partitionsUnchanged()) {
      positioned = false;
    }
    if (!positioned && !position()) {
      return false;
    }
    final ConsumerRecords<byte[], byte[]> records;
    try {
      records = consumer.poll(settings.pollTimeout());
    } catch (InvalidOffsetException e) {
      log.warn("policy change listener lost its position on {}; loading again", e.partitions());
      positioned = false;
      return true;
    }
    if (!records.isEmpty()) {
      onRecords(records);
    }
    return true;
  }

  /**
   * False when the topic is gone or its partitions differ from the assigned ones. Then the listener
   * positions again: it assigns all partitions and loads, so changes on added partitions are read.
   */
  private boolean partitionsUnchanged() {
    nextTopicCheckMillis = clock.millis() + settings.topicCheckInterval().toMillis();
    // This runs on the poll thread, so use a short timeout: a slow broker must not stop polling
    // for long. A timeout is not a change: keep the current assignment and check again later.
    final List<PartitionInfo> partitions;
    try {
      partitions = consumer.partitionsFor(topic, TOPIC_CHECK_TIMEOUT);
    } catch (TimeoutException e) {
      log.warn(
          "policy change topic check timed out after {} s; checking again later",
          TOPIC_CHECK_TIMEOUT.toSeconds());
      return true;
    }
    if (partitions == null || partitions.isEmpty()) {
      return false;
    }
    final Set<TopicPartition> current =
        partitions.stream()
            .map(p -> new TopicPartition(p.topic(), p.partition()))
            .collect(Collectors.toSet());
    if (!current.equals(consumer.assignment())) {
      log.info(
          "policy change topic {} partitions changed from {} to {}; loading again",
          topic,
          consumer.assignment().size(),
          current.size());
      return false;
    }
    return true;
  }

  private boolean position() throws Exception {
    final List<PartitionInfo> partitions = consumer.partitionsFor(topic, settings.loadTimeout());
    if (partitions == null || partitions.isEmpty()) {
      if (!missingTopicLogged) {
        log.warn(
            "policy change topic {} not found; policies refresh on the timer until it exists",
            topic);
        missingTopicLogged = true;
      }
      consumer.unsubscribe();
      return false;
    }
    missingTopicLogged = false;
    final List<TopicPartition> assigned =
        partitions.stream()
            .map(p -> new TopicPartition(p.topic(), p.partition()))
            .collect(Collectors.toList());
    consumer.assign(assigned);
    final Map<TopicPartition, Long> ends = consumer.endOffsets(assigned, settings.loadTimeout());
    final long replayFrom = clock.millis() - settings.replayWindow().toMillis();
    final Map<TopicPartition, Long> replayTimes = new HashMap<>();
    for (TopicPartition tp : assigned) {
      replayTimes.put(tp, replayFrom);
    }
    final Map<TopicPartition, OffsetAndTimestamp> replayStarts =
        consumer.offsetsForTimes(replayTimes, settings.loadTimeout());

    awaitLoad(loadRequester.get());

    // The full load may miss a change the search index does not list yet. Reading the messages of
    // the replay window again reads those policies by key. A partition with no message in the
    // window starts at its end offset.
    final Map<TopicPartition, Long> starts = new HashMap<>();
    for (TopicPartition tp : assigned) {
      final OffsetAndTimestamp start = replayStarts == null ? null : replayStarts.get(tp);
      starts.put(tp, start != null ? start.offset() : ends.getOrDefault(tp, 0L));
      consumer.seek(tp, starts.get(tp));
    }
    replayEnds = ends;
    positioned = true;
    nextTopicCheckMillis = clock.millis() + settings.topicCheckInterval().toMillis();
    if (positionedBefore) {
      increment("policyChange.resync", 1);
    }
    positionedBefore = true;
    log.info(
        "policy change listener reading {} from offsets {} (end offsets {})", topic, starts, ends);
    return true;
  }

  /**
   * Waits for the start load in short steps, so close() does not have to wait for a slow load. The
   * load itself continues on the authorizer's load thread.
   */
  private void awaitLoad(@Nonnull CompletableFuture<Void> load) throws Exception {
    final long deadline = System.nanoTime() + settings.loadTimeout().toNanos();
    while (true) {
      if (!running && thread != null) {
        throw new WakeupException();
      }
      final long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        throw new java.util.concurrent.TimeoutException(
            "policy load did not finish within " + settings.loadTimeout());
      }
      try {
        load.get(Math.min(remaining, LOAD_WAIT_STEP.toNanos()), TimeUnit.NANOSECONDS);
        return;
      } catch (java.util.concurrent.TimeoutException e) {
        // Check running and the deadline again.
      }
    }
  }

  private void onRecords(ConsumerRecords<byte[], byte[]> records) {
    final int count = records.count();
    final Set<Urn> urns = new HashSet<>();
    int unreadable = 0;
    int replayed = 0;
    long oldest = Long.MAX_VALUE;
    for (ConsumerRecord<byte[], byte[]> record : records) {
      final Urn urn = policyUrn(record.key());
      if (urn != null) {
        urns.add(urn);
      } else {
        unreadable++;
      }
      final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
      if (record.offset() < replayEnds.getOrDefault(tp, 0L)) {
        replayed++;
      } else if (record.timestamp() >= 0) {
        oldest = Math.min(oldest, record.timestamp());
      }
    }
    increment("policyChange.consumed", count);
    if (replayed > 0) {
      increment("policyChange.replayed", replayed);
    }
    // Policy changes are rare, so one line for each batch is cheap and shows that changes arrive.
    log.info(
        "policy change listener read {} policy changes ({} replayed); updating the policy cache",
        count,
        replayed);
    final long oldestTimestamp = oldest;
    if (unreadable > 0) {
      increment("policyChange.unreadable_key", unreadable);
      log.warn(
          "policy change listener read {} messages whose key is not a policy URN; loading all"
              + " policies",
          unreadable);
    }
    // Do not wait: requests are ordered and joined, so a new batch gets one later read. With an
    // unreadable key, both are requested: they usually merge into one full load that also reads the
    // good
    // URNs by key, so those still go on the recent-changes list.
    final CompletableFuture<Void> update =
        urns.isEmpty() ? CompletableFuture.completedFuture(null) : updateRequester.apply(urns);
    final CompletableFuture<Void> applied =
        unreadable > 0 ? CompletableFuture.allOf(loadRequester.get(), update) : update;
    applied.whenComplete(
        (ignored, error) -> {
          if (error != null) {
            log.warn("policy cache update after {} policy changes failed", count, error);
            return;
          }
          increment("policyChange.applied", count);
          // Replayed messages are old by design; only new ones measure the delay.
          if (metricUtils != null && oldestTimestamp != Long.MAX_VALUE) {
            metricUtils.histogram(
                PolicyChangeListener.class,
                "policyChange.apply_delay",
                Math.max(0, clock.millis() - oldestTimestamp));
          }
        });
  }

  /** The policy URN in a message key, or null when the key is not a dataHubPolicy URN. */
  @Nullable
  static Urn policyUrn(@Nullable byte[] key) {
    if (key == null) {
      return null;
    }
    try {
      final Urn urn = UrnUtils.getUrn(new String(key, StandardCharsets.UTF_8));
      return POLICY_ENTITY_NAME.equals(urn.getEntityType()) ? urn : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  private void increment(String name, int count) {
    if (metricUtils != null) {
      metricUtils.increment(PolicyChangeListener.class, name, count);
    }
  }
}
