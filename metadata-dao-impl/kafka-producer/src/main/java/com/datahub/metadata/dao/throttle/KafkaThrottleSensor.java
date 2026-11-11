package com.datahub.metadata.dao.throttle;

import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_TIMESERIES_LAG;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_VERSIONED_LAG;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleEvent;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * This class is designed to monitor MCL consumption by a specific consumer group and provide
 * throttling hooks.
 *
 * <p>Initially this was designed for throttling the async mcp processor `mce-consumer`, however it
 * also handles throttling synchronous requests via rest.li, graphql, and openapi for non-browser
 * based requests.
 */
@Slf4j
@Builder(toBuilder = true)
public class KafkaThrottleSensor implements ThrottleSensor, AutoCloseable {
  private static final Set<ThrottleType> SUPPORTED_THROTTLE_TYPES =
      Set.of(MCL_VERSIONED_LAG, MCL_TIMESERIES_LAG);
  @Nonnull private final EntityRegistry entityRegistry;
  @Nonnull private final Admin kafkaAdmin;
  @Nonnull private final MetadataChangeProposalConfig.ThrottlesConfig config;
  @Nonnull private final String mclConsumerGroupId;
  @Nonnull private final String versionedTopicName;
  @Nonnull private final String timeseriesTopicName;
  private final MetricUtils metricUtils;

  /** A list of throttle event listeners to execute when throttling occurs and ceases */
  /** How long close() waits for a refresh in flight, and then for the admin client, to finish */
  @Builder.Default @Nonnull private final Duration shutdownTimeout = Duration.ofSeconds(5);

  @Builder.Default @Nonnull
  private final List<Function<ThrottleEvent, ThrottleControl>> throttleCallbacks =
      new ArrayList<>();

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final Map<ThrottleType, Long> medianLag = new ConcurrentHashMap<>();
  private final Map<ThrottleType, BackOffExecution> backoffMap = new ConcurrentHashMap<>();

  /** Topics whose last refresh found no committed offsets, so their log line is written once. */
  private final Set<ThrottleType> topicsWithoutOffsets = ConcurrentHashMap.newKeySet();

  /** Last lag computed from committed offsets, per topic, for the log line when they vanish. */
  private final Map<ThrottleType, Long> lastLagWithOffsets = new ConcurrentHashMap<>();

  @Override
  public KafkaThrottleSensor addCallback(Function<ThrottleEvent, ThrottleControl> callback) {
    throttleCallbacks.add(callback);
    return this;
  }

  /** Update lag information at a given rate */
  public KafkaThrottleSensor start() {
    if ((config.getVersioned().isEnabled() || config.getTimeseries().isEnabled())
        && config.getUpdateIntervalMs() > 0) {
      scheduler.scheduleAtFixedRate(
          () -> {
            try {
              refresh();
              throttle();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } catch (RuntimeException e) {
              // An exception escaping this task would cancel it and stop lag refreshes for the
              // life of the process, so log it and try again at the next interval.
              log.error("MCL lag refresh or throttle failed; retrying at the next interval.", e);
            }
          },
          config.getUpdateIntervalMs(),
          config.getUpdateIntervalMs(),
          TimeUnit.MILLISECONDS);
    }
    return this;
  }

  @VisibleForTesting
  public void refresh() {
    medianLag.putAll(getMedianLag());
    log.info("MCL medianLag: {}", medianLag);
  }

  @VisibleForTesting
  public void stop() {
    scheduler.shutdown();
  }

  /**
   * Stops the lag refresh and closes the admin client this sensor owns. Spring calls this when the
   * context closes.
   */
  @Override
  public void close() {
    // shutdownNow interrupts a throttle() sleeping through a backoff. Wait for a refresh already
    // in flight so it is not still using the admin client when it closes.
    scheduler.shutdownNow();
    try {
      if (!scheduler.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        log.warn("MCL lag refresh did not stop within {}.", shutdownTimeout);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    kafkaAdmin.close(shutdownTimeout);
  }

  /**
   * Get copy of the lag info
   *
   * @return median lag per mcl topic
   */
  @VisibleForTesting
  public Map<ThrottleType, Long> getLag() {
    return medianLag.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @VisibleForTesting
  public boolean isThrottled(ThrottleType mclType) {
    if (getThrottleConfig(mclType).isEnabled() && medianLag.containsKey(mclType)) {
      return medianLag.get(mclType) > getThrottleConfig(mclType).getThreshold();
    }
    return false;
  }

  @VisibleForTesting
  public long computeNextBackOff(ThrottleType mclType) {
    if (isThrottled(mclType)) {
      BackOffExecution backOffExecution =
          backoffMap.computeIfAbsent(
              mclType,
              k -> {
                MetadataChangeProposalConfig.ThrottleConfig throttleConfig =
                    getThrottleConfig(mclType);
                ExponentialBackOff backoff =
                    new ExponentialBackOff(
                        throttleConfig.getInitialIntervalMs(), throttleConfig.getMultiplier());
                backoff.setMaxAttempts(throttleConfig.getMaxAttempts());
                backoff.setMaxInterval(throttleConfig.getMaxIntervalMs());
                return backoff.start();
              });
      return backOffExecution.nextBackOff();
    }
    return 0;
  }

  @VisibleForTesting
  public void throttle() throws InterruptedException {

    Map<ThrottleType, Long> throttled = new LinkedHashMap<>();

    for (ThrottleType mclType : SUPPORTED_THROTTLE_TYPES) {
      long backoffWaitMs = computeNextBackOff(mclType);

      if (backoffWaitMs <= 0) {
        // not throttled, remove backoff tracking
        log.info("Throttle exponential backoff reset.");
        backoffMap.remove(mclType);
        if (metricUtils != null)
          metricUtils.setGaugeValue(
              this.getClass(), String.format("%s_throttled", getTopicName(mclType)), 0);
      } else {
        throttled.put(mclType, backoffWaitMs);
      }
    }

    // handle throttled
    if (!throttled.isEmpty()) {
      long maxBackoffWaitMs = throttled.values().stream().max(Comparator.naturalOrder()).get();
      log.warn(
          "Throttled Topic: {} Duration: {} ms MedianLag: {}",
          throttled.keySet().stream().map(this::getTopicName).collect(Collectors.toList()),
          maxBackoffWaitMs,
          throttled.keySet().stream().map(medianLag::get).collect(Collectors.toList()));

      throttled.keySet().stream()
          .forEach(
              mclType -> {
                if (metricUtils != null)
                  metricUtils.setGaugeValue(
                      this.getClass(), String.format("%s_throttled", getTopicName(mclType)), 1);
                if (metricUtils != null)
                  metricUtils.increment(
                      this.getClass(),
                      String.format("%s_throttledCount", getTopicName(mclType)),
                      1);
              });

      log.info("Throttling {} callbacks for {} ms.", throttleCallbacks.size(), maxBackoffWaitMs);
      final ThrottleEvent throttleEvent = ThrottleEvent.throttle(throttled);
      List<ThrottleControl> throttleControls =
          throttleCallbacks.stream().map(callback -> callback.apply(throttleEvent)).toList();

      if (throttleControls.stream().anyMatch(ThrottleControl::hasCallback)) {
        Thread.sleep(maxBackoffWaitMs);
        log.info("Resuming {} callbacks after wait.", throttleControls.size());
        throttleControls.forEach(
            control -> control.execute(ThrottleEvent.clearThrottle(throttleEvent)));
      }
    }
  }

  private Map<ThrottleType, Long> getMedianLag() {
    try {
      Map<TopicPartition, OffsetAndMetadata> mclConsumerOffsets =
          kafkaAdmin
              .listConsumerGroupOffsets(mclConsumerGroupId)
              .partitionsToOffsetAndMetadata()
              .get()
              .entrySet()
              .stream()
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      Map<TopicPartition, OffsetSpec> latestOffsetRequest =
          mclConsumerOffsets.keySet().stream()
              .map(offsetAndMetadata -> Map.entry(offsetAndMetadata, OffsetSpec.latest()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      Map<TopicPartition, Long> endOffsetValues =
          kafkaAdmin.listOffsets(latestOffsetRequest).all().get().entrySet().stream()
              .map(entry -> Map.entry(entry.getKey(), entry.getValue().offset()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      return Stream.of(
              Pair.of(MCL_VERSIONED_LAG, versionedTopicName),
              Pair.of(MCL_TIMESERIES_LAG, timeseriesTopicName))
          .map(
              topic -> {
                ThrottleType mclType = topic.getFirst();
                String topicName = topic.getSecond();

                Map<TopicPartition, OffsetAndMetadata> topicOffsets =
                    mclConsumerOffsets.entrySet().stream()
                        .filter(entry -> entry.getKey().topic().equals(topicName))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (topicOffsets.isEmpty()) {
                  logOnceWithoutOffsets(mclType, topicName);
                  return Map.entry(mclType, 0L);
                }
                if (topicsWithoutOffsets.remove(mclType)) {
                  log.info(
                      "Consumer group {} has committed offsets on MCL topic {}; measuring its lag.",
                      mclConsumerGroupId,
                      topicName);
                }
                List<Double> offsetValues =
                    topicOffsets.values().stream()
                        .map(OffsetAndMetadata::offset)
                        .map(Long::doubleValue)
                        .collect(Collectors.toList());
                long offsetMedian = getMedian(offsetValues).longValue();

                List<Double> topicEndOffsetValues =
                    topicOffsets.keySet().stream()
                        .map(topicPart -> endOffsetValues.getOrDefault(topicPart, 0L))
                        .map(Long::doubleValue)
                        .collect(Collectors.toList());
                long endOffsetMedian = getMedian(topicEndOffsetValues).longValue();
                long lag = Math.max(0, endOffsetMedian - offsetMedian);
                lastLagWithOffsets.put(mclType, lag);
                return Map.entry(mclType, lag);
              })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } catch (InterruptedException e) {
      // close() interrupts a refresh in flight; this is shutdown, not a failure
      Thread.currentThread().interrupt();
      return Map.of(MCL_VERSIONED_LAG, 0L, MCL_TIMESERIES_LAG, 0L);
    } catch (ExecutionException e) {
      log.error("Error fetching consumer group offsets.", e);
      return Map.of(MCL_VERSIONED_LAG, 0L, MCL_TIMESERIES_LAG, 0L);
    }
  }

  /**
   * A topic with no committed offsets reports zero lag, which turns lag throttling off for it. That
   * covers a fresh install or a topic the group hasn't consumed yet, and also a group whose offsets
   * were removed: expired after the group had no members for {@code offsets.retention.minutes}, or
   * after it stopped consuming the topic, or deleted by an admin.
   *
   * <p>Zero is reported even when a lag was seen before, rather than keeping that last value.
   * Nothing refreshes a kept value until the group commits on the topic again, so a group that
   * stopped consuming the topic would throttle writers for good. When consumers come back they
   * start from {@code auto.offset.reset} (earliest by default) and commit, and the next refresh
   * measures the real backlog and throttles again.
   *
   * <p>Logged once per transition. It is a WARN only when this process saw offsets on the topic
   * before and throttles it, the case where throttling silently stops; a fresh install or a topic
   * whose throttling is off gets an INFO. What was seen lives in memory only, so a pod that starts
   * after the offsets are gone logs the INFO.
   */
  private void logOnceWithoutOffsets(ThrottleType mclType, String topicName) {
    if (!topicsWithoutOffsets.add(mclType)) {
      return;
    }
    Long lastLag = lastLagWithOffsets.get(mclType);
    if (lastLag != null && getThrottleConfig(mclType).isEnabled()) {
      log.warn(
          "Consumer group {} no longer has committed offsets on MCL topic {}; its last median lag"
              + " was {} messages. Reporting zero lag, so writers are not throttled for this topic"
              + " until the group commits on it again. Check that the MCL consumers are running:"
              + " offsets expire after the group has no members for offsets.retention.minutes.",
          mclConsumerGroupId,
          topicName,
          lastLag);
    } else {
      log.info(
          "Consumer group {} has no committed offsets on MCL topic {}; reporting zero lag until it"
              + " commits.",
          mclConsumerGroupId,
          topicName);
    }
  }

  private MetadataChangeProposalConfig.ThrottleConfig getThrottleConfig(ThrottleType mclType) {
    MetadataChangeProposalConfig.ThrottleConfig throttleConfig;
    switch (mclType) {
      case MCL_VERSIONED_LAG -> throttleConfig = config.getVersioned();
      case MCL_TIMESERIES_LAG -> throttleConfig = config.getTimeseries();
      default -> throw new IllegalStateException();
    }
    return throttleConfig;
  }

  private String getTopicName(ThrottleType mclType) {
    return MCL_TIMESERIES_LAG.equals(mclType) ? timeseriesTopicName : versionedTopicName;
  }

  private static Double getMedian(Collection<Double> listValues) {
    double[] values = listValues.stream().mapToDouble(d -> d).sorted().toArray();
    double median;
    if (values.length % 2 == 0)
      median = (values[values.length / 2] + values[values.length / 2 - 1]) / 2;
    else median = values[values.length / 2];
    return median;
  }
}
