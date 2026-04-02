package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Computes lag for the MAE/MCL consumer group across the versioned and timeseries MCL topics, using
 * the same median-offset approach as {@code KafkaThrottleSensor}.
 */
public final class MclConsumerLagUtils {

  public static final String DEFAULT_MCL_CONSUMER_GROUP_ID = "generic-mae-consumer-job-client";

  private MclConsumerLagUtils() {}

  /**
   * Sum of per-topic median lag for the two MCL topics. Returns 0 if the consumer has no committed
   * offsets for those topics (e.g. group unknown or not yet consuming).
   */
  public static long combinedMedianLag(
      @Nonnull Admin kafkaAdmin,
      @Nonnull String consumerGroupId,
      @Nonnull String versionedTopicName,
      @Nonnull String timeseriesTopicName)
      throws Exception {

    Map<TopicPartition, OffsetAndMetadata> committed =
        kafkaAdmin.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get();

    long versionedLag = medianLagForTopic(committed, kafkaAdmin, versionedTopicName);
    long timeseriesLag = medianLagForTopic(committed, kafkaAdmin, timeseriesTopicName);
    return versionedLag + timeseriesLag;
  }

  private static long medianLagForTopic(
      Map<TopicPartition, OffsetAndMetadata> committed, Admin kafkaAdmin, String topicName)
      throws Exception {

    Map<TopicPartition, OffsetAndMetadata> topicOffsets =
        committed.entrySet().stream()
            .filter(e -> e.getKey().topic().equals(topicName))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (topicOffsets.isEmpty()) {
      return 0L;
    }

    Map<TopicPartition, OffsetSpec> latestRequest =
        topicOffsets.keySet().stream()
            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

    Map<TopicPartition, Long> endOffsets =
        kafkaAdmin.listOffsets(latestRequest).all().get().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

    Collection<Double> consumerOffsets =
        topicOffsets.values().stream()
            .map(OffsetAndMetadata::offset)
            .map(Long::doubleValue)
            .toList();
    Collection<Double> endOffsetVals =
        topicOffsets.keySet().stream()
            .map(tp -> endOffsets.getOrDefault(tp, 0L).doubleValue())
            .toList();

    long offsetMedian = median(consumerOffsets).longValue();
    long endMedian = median(endOffsetVals).longValue();
    return Math.max(0, endMedian - offsetMedian);
  }

  private static Double median(Collection<Double> values) {
    double[] sorted = values.stream().mapToDouble(d -> d).sorted().toArray();
    if (sorted.length == 0) {
      return 0.0;
    }
    if (sorted.length % 2 == 0) {
      return (sorted[sorted.length / 2] + sorted[sorted.length / 2 - 1]) / 2;
    }
    return sorted[sorted.length / 2];
  }
}
