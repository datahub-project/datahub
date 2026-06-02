package com.datahub.metadata.dao.throttle;

import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_TIMESERIES_LAG;
import static com.linkedin.metadata.dao.throttle.ThrottleType.MCL_VERSIONED_LAG;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleEvent;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import com.linkedin.metadata.queue.MetadataQueueStore;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * pgQueue-backed MCL lag sensor mirroring {@link KafkaThrottleSensor} thresholds and callbacks,
 * using {@link MetadataQueueStore} committed offsets and log high-water marks.
 */
@Slf4j
@Builder(toBuilder = true)
public class PgQueueThrottleSensor implements ThrottleSensor {

  private static final Set<ThrottleType> SUPPORTED_THROTTLE_TYPES =
      Set.of(MCL_VERSIONED_LAG, MCL_TIMESERIES_LAG);

  @Nonnull private final MetadataQueueStore metadataQueueStore;
  @Nonnull private final MetadataChangeProposalConfig.ThrottlesConfig config;
  @Nonnull private final String mclConsumerGroupId;
  @Nonnull private final String versionedTopicName;
  @Nonnull private final String timeseriesTopicName;
  private final MetricUtils metricUtils;

  @Builder.Default @Nonnull
  private final List<Function<ThrottleEvent, ThrottleControl>> throttleCallbacks =
      new ArrayList<>();

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final Map<ThrottleType, Long> medianLag = new ConcurrentHashMap<>();
  private final Map<ThrottleType, BackOffExecution> backoffMap = new ConcurrentHashMap<>();

  @Override
  public PgQueueThrottleSensor addCallback(Function<ThrottleEvent, ThrottleControl> callback) {
    throttleCallbacks.add(callback);
    return this;
  }

  public PgQueueThrottleSensor start() {
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
    log.info("MCL medianLag (pgQueue): {}", medianLag);
  }

  @VisibleForTesting
  public void stop() {
    scheduler.shutdown();
  }

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
        log.info("Throttle exponential backoff reset.");
        backoffMap.remove(mclType);
        if (metricUtils != null) {
          metricUtils.setGaugeValue(
              this.getClass(), String.format("%s_throttled", getTopicName(mclType)), 0);
        }
      } else {
        throttled.put(mclType, backoffWaitMs);
      }
    }
    if (!throttled.isEmpty()) {
      long maxBackoffWaitMs = throttled.values().stream().max(Comparator.naturalOrder()).get();
      log.warn(
          "Throttled Topic: {} Duration: {} ms MedianLag: {}",
          throttled.keySet().stream().map(this::getTopicName).collect(Collectors.toList()),
          maxBackoffWaitMs,
          throttled.keySet().stream().map(medianLag::get).collect(Collectors.toList()));
      throttled
          .keySet()
          .forEach(
              mclType -> {
                if (metricUtils != null) {
                  metricUtils.setGaugeValue(
                      this.getClass(), String.format("%s_throttled", getTopicName(mclType)), 1);
                  metricUtils.increment(
                      this.getClass(),
                      String.format("%s_throttledCount", getTopicName(mclType)),
                      1);
                }
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
    return Stream.of(
            Pair.of(MCL_VERSIONED_LAG, versionedTopicName),
            Pair.of(MCL_TIMESERIES_LAG, timeseriesTopicName))
        .map(this::medianLagForTopicPair)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map.Entry<ThrottleType, Long> medianLagForTopicPair(Pair<ThrottleType, String> topic) {
    ThrottleType mclType = topic.getFirst();
    String topicName = topic.getSecond();
    return metadataQueueStore
        .fetchTopic(topicName)
        .map(
            meta -> {
              long topicId = meta.id();
              int partitionCount = meta.partitionCount();
              Map<Integer, Long> maxSeqs =
                  metadataQueueStore.partitionMaxEnqueueSeqs(topicId, partitionCount);
              List<Double> lags = new ArrayList<>();
              for (int p = 0; p < partitionCount; p++) {
                long lastSeq = maxSeqs.getOrDefault(p, 0L);
                long committed =
                    metadataQueueStore.getCommittedOffset(mclConsumerGroupId, topicId, p);
                long aheadBy = Math.max(0L, committed - lastSeq);
                if (aheadBy > 0) {
                  lags.add((double) Long.MAX_VALUE / 4);
                } else {
                  lags.add((double) Math.max(0L, lastSeq - committed));
                }
              }
              if (lags.isEmpty()) {
                return Map.entry(mclType, 0L);
              }
              return Map.entry(mclType, getMedian(lags).longValue());
            })
        .orElseGet(() -> Map.entry(mclType, 0L));
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
    if (values.length % 2 == 0) {
      median = (values[values.length / 2] + values[values.length / 2 - 1]) / 2;
    } else {
      median = values[values.length / 2];
    }
    return median;
  }
}
