package com.linkedin.gms.factory.auth;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Reads the policy change topic in every process with a policy cache, and reloads the cache when a
 * policy changes anywhere.
 *
 * <p>The listener needs no consumer group and commits no offsets: each process reads every message
 * and only ever reads from the end. The message content is not used; any message means "reload".
 *
 * <p>How it starts reading, without a gap:
 *
 * <ol>
 *   <li>assign all partitions of the topic;
 *   <li>record their end offsets;
 *   <li>run a full load (it sees every change committed before those offsets);
 *   <li>seek to the recorded offsets, so every later change causes another load.
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
      @Nonnull Duration loadTimeout) {
    public static Settings defaults() {
      return new Settings(
          Duration.ofSeconds(1),
          Duration.ofSeconds(30),
          Duration.ofSeconds(30),
          Duration.ofSeconds(90));
    }
  }

  private static final Duration TOPIC_CHECK_TIMEOUT = Duration.ofSeconds(10);

  private final Consumer<byte[], byte[]> consumer;
  private final String topic;
  private final Supplier<CompletableFuture<Void>> loadRequester;
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

  public PolicyChangeListener(
      @Nonnull Consumer<byte[], byte[]> consumer,
      @Nonnull String topic,
      @Nonnull Supplier<CompletableFuture<Void>> loadRequester,
      @Nullable MetricUtils metricUtils,
      @Nonnull Settings settings,
      @Nonnull Clock clock) {
    this.consumer = consumer;
    this.topic = topic;
    this.loadRequester = loadRequester;
    this.metricUtils = metricUtils;
    this.settings = settings;
    this.clock = clock;
  }

  /** Starts the listener thread. The process does not wait for it. */
  public synchronized void start() {
    if (thread != null) {
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

    // The end offsets are recorded first, so this load sees every change before them, and the
    // seek below reads every change after them. No change falls between the two.
    loadRequester.get().get(settings.loadTimeout().toMillis(), TimeUnit.MILLISECONDS);

    for (TopicPartition tp : assigned) {
      consumer.seek(tp, ends.getOrDefault(tp, 0L));
    }
    positioned = true;
    nextTopicCheckMillis = clock.millis() + settings.topicCheckInterval().toMillis();
    if (positionedBefore) {
      increment("policyChange.resync", 1);
    }
    positionedBefore = true;
    log.info("policy change listener reading {} from offsets {}", topic, ends);
    return true;
  }

  private void onRecords(ConsumerRecords<byte[], byte[]> records) {
    final int count = records.count();
    long oldest = Long.MAX_VALUE;
    for (ConsumerRecord<byte[], byte[]> record : records) {
      if (record.timestamp() >= 0) {
        oldest = Math.min(oldest, record.timestamp());
      }
    }
    increment("policyChange.consumed", count);
    // Policy changes are rare, so one line for each batch is cheap and shows that changes arrive.
    log.info("policy change listener read {} policy changes; reloading the policy cache", count);
    final long oldestTimestamp = oldest;
    // Do not wait: loads are ordered and joined, so a new batch during a load gets one later load.
    loadRequester
        .get()
        .whenComplete(
            (ignored, error) -> {
              if (error != null) {
                log.warn("policy cache load after {} policy changes failed", count, error);
                return;
              }
              increment("policyChange.applied", count);
              if (metricUtils != null && oldestTimestamp != Long.MAX_VALUE) {
                metricUtils.histogram(
                    PolicyChangeListener.class,
                    "policyChange.apply_delay",
                    Math.max(0, clock.millis() - oldestTimestamp));
              }
            });
  }

  private void increment(String name, int count) {
    if (metricUtils != null) {
      metricUtils.increment(PolicyChangeListener.class, name, count);
    }
  }
}
