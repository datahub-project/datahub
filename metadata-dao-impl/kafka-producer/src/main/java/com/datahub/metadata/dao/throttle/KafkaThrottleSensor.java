package com.datahub.metadata.dao.throttle;

import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_TIMESERIES_LAG;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_VERSIONED_LAG;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleEvent;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
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
public class KafkaThrottleSensor implements ThrottleSensor {
  private static final Set<ThrottleType> SUPPORTED_THROTTLE_TYPES =
      Set.of(MCL_VERSIONED_LAG, MCL_TIMESERIES_LAG);
  @Nonnull private final EntityRegistry entityRegistry;
  @Nonnull private final Admin kafkaAdmin;
  @Nonnull private final MetadataChangeProposalConfig.ThrottlesConfig config;
  @Nonnull private final String mclConsumerGroupId;
  @Nonnull private final String versionedTopicName;
  @Nonnull private final String timeseriesTopicName;

  /** A list of throttle event listeners to execute when throttling occurs and ceases */
  @Builder.Default @Nonnull
  private final List<Function<ThrottleEvent, ThrottleControl>> throttleCallbacks =
      new ArrayList<>();

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final Map<ThrottleType, Long> medianLag = new ConcurrentHashMap<>();
  private final Map<ThrottleType, BackOffExecution> backoffMap = new ConcurrentHashMap<>();

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
            refresh();
            try {
              throttle();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
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
        MetricUtils.gauge(
            this.getClass(),
            String.format("%s_throttled", getTopicName(mclType)),
            () -> (Gauge<?>) () -> 0);
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
                MetricUtils.gauge(
                    this.getClass(),
                    String.format("%s_throttled", getTopicName(mclType)),
                    () -> (Gauge<?>) () -> 1);
                MetricUtils.counter(
                        this.getClass(), String.format("%s_throttledCount", getTopicName(mclType)))
                    .inc();
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
                return Map.entry(mclType, Math.max(0, endOffsetMedian - offsetMedian));
              })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } catch (ExecutionException | InterruptedException e) {
      log.error("Error fetching consumer group offsets.", e);
      return Map.of(MCL_VERSIONED_LAG, 0L, MCL_TIMESERIES_LAG, 0L);
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
