package com.datahub.metadata.dao.producer;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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

@Slf4j
@Builder(toBuilder = true)
public class KafkaProducerThrottle {
  @Nonnull private final EntityRegistry entityRegistry;
  @Nonnull private final Admin kafkaAdmin;
  @Nonnull private final MetadataChangeProposalConfig.ThrottlesConfig config;
  @Nonnull private final String mclConsumerGroupId;
  @Nonnull private final String versionedTopicName;
  @Nonnull private final String timeseriesTopicName;
  @Nonnull private final Consumer<Boolean> pauseConsumer;

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final Map<MclType, Long> medianLag = new ConcurrentHashMap<>();
  private final Map<MclType, BackOffExecution> backoffMap = new ConcurrentHashMap<>();

  /** Update lag information at a given rate */
  public KafkaProducerThrottle start() {
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
  public Map<MclType, Long> getLag() {
    return medianLag.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @VisibleForTesting
  public boolean isThrottled(MclType mclType) {
    if (getThrottleConfig(mclType).isEnabled() && medianLag.containsKey(mclType)) {
      return medianLag.get(mclType) > getThrottleConfig(mclType).getThreshold();
    }
    return false;
  }

  @VisibleForTesting
  public long computeNextBackOff(MclType mclType) {
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
    for (MclType mclType : MclType.values()) {
      if (isThrottled(mclType)) {
        long backoffWaitMs = computeNextBackOff(mclType);

        if (backoffWaitMs > 0) {
          log.warn(
              "Throttled producer Topic: {} Duration: {} ms MedianLag: {}",
              getTopicName(mclType),
              backoffWaitMs,
              medianLag.get(mclType));
          MetricUtils.gauge(
              this.getClass(),
              String.format("%s_throttled", getTopicName(mclType)),
              () -> (Gauge<?>) () -> 1);
          MetricUtils.counter(
                  this.getClass(), String.format("%s_throttledCount", getTopicName(mclType)))
              .inc();

          log.info("Pausing MCE consumer for {} ms.", backoffWaitMs);
          pauseConsumer.accept(true);
          Thread.sleep(backoffWaitMs);
          log.info("Resuming MCE consumer.");
          pauseConsumer.accept(false);

          // if throttled for one topic, skip remaining
          return;
        } else {
          // no throttle or exceeded configuration limits
          log.info("MCE consumer throttle exponential backoff reset.");
          backoffMap.remove(mclType);
          MetricUtils.gauge(
              this.getClass(),
              String.format("%s_throttled", getTopicName(mclType)),
              () -> (Gauge<?>) () -> 0);
        }
      } else {
        // not throttled, remove backoff tracking
        log.info("MCE consumer throttle exponential backoff reset.");
        backoffMap.remove(mclType);
        MetricUtils.gauge(
            this.getClass(),
            String.format("%s_throttled", getTopicName(mclType)),
            () -> (Gauge<?>) () -> 0);
      }
    }
  }

  private Map<MclType, Long> getMedianLag() {
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
              Pair.of(MclType.VERSIONED, versionedTopicName),
              Pair.of(MclType.TIMESERIES, timeseriesTopicName))
          .map(
              topic -> {
                MclType mclType = topic.getFirst();
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
      return Map.of(MclType.VERSIONED, 0L, MclType.TIMESERIES, 0L);
    }
  }

  private MetadataChangeProposalConfig.ThrottleConfig getThrottleConfig(MclType mclType) {
    MetadataChangeProposalConfig.ThrottleConfig throttleConfig;
    switch (mclType) {
      case VERSIONED -> throttleConfig = config.getVersioned();
      case TIMESERIES -> throttleConfig = config.getTimeseries();
      default -> throw new IllegalStateException();
    }
    return throttleConfig;
  }

  private String getTopicName(MclType mclType) {
    return MclType.TIMESERIES.equals(mclType) ? timeseriesTopicName : versionedTopicName;
  }

  private static Double getMedian(Collection<Double> listValues) {
    double[] values = listValues.stream().mapToDouble(d -> d).sorted().toArray();
    double median;
    if (values.length % 2 == 0)
      median = (values[values.length / 2] + values[values.length / 2 - 1]) / 2;
    else median = values[values.length / 2];
    return median;
  }

  public enum MclType {
    TIMESERIES,
    VERSIONED
  }
}
