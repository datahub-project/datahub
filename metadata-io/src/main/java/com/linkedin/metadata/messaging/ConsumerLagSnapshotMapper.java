package com.linkedin.metadata.messaging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Builds {@link ConsumerGroupLagSnapshot} from Kafka admin/consumer offset maps. */
public final class ConsumerLagSnapshotMapper {

  private ConsumerLagSnapshotMapper() {}

  @Nonnull
  public static ConsumerGroupLagSnapshot fromKafkaOffsets(
      @Nullable String consumerGroupId,
      @Nonnull Map<TopicPartition, OffsetAndMetadata> offsetMap,
      @Nonnull Map<TopicPartition, Long> endOffsets,
      boolean detailed) {
    if (consumerGroupId == null || offsetMap == null || offsetMap.isEmpty()) {
      return ConsumerGroupLagSnapshot.builder()
          .consumerGroupId(consumerGroupId != null ? consumerGroupId : "")
          .build();
    }

    Map<String, Map<Integer, PartitionLagSnapshot>> topicToPartitions = new HashMap<>();
    Map<String, List<Long>> topicToLags = new HashMap<>();

    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetMap.entrySet()) {
      TopicPartition tp = entry.getKey();
      OffsetAndMetadata offset = entry.getValue();
      String topic = tp.topic();
      int partition = tp.partition();
      long consumerOffset = offset.offset();
      Long endOffset = endOffsets.get(tp);
      Long lag = (endOffset != null) ? Math.max(0, endOffset - consumerOffset) : null;
      String metadata =
          offset.metadata() != null && !offset.metadata().isEmpty() ? offset.metadata() : null;
      PartitionLagSnapshot part =
          PartitionLagSnapshot.builder().offset(consumerOffset).lag(lag).metadata(metadata).build();
      topicToPartitions.computeIfAbsent(topic, t -> new HashMap<>()).put(partition, part);
      if (lag != null) {
        topicToLags.computeIfAbsent(topic, t -> new ArrayList<>()).add(lag);
      }
    }

    Map<String, TopicLagSnapshot> topics = new LinkedHashMap<>();
    topicToPartitions.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            topicEntry -> {
              String topic = topicEntry.getKey();
              Map<Integer, PartitionLagSnapshot> partitionMap = topicEntry.getValue();
              Map<String, PartitionLagSnapshot> sortedPartitions = new LinkedHashMap<>();
              partitionMap.entrySet().stream()
                  .sorted(Map.Entry.comparingByKey())
                  .forEach(e -> sortedPartitions.put(String.valueOf(e.getKey()), e.getValue()));
              Optional<LagMetricsSnapshot> metrics =
                  Optional.ofNullable(calculateLagMetrics(topicToLags.get(topic)));
              topics.put(
                  topic,
                  TopicLagSnapshot.builder()
                      .partitions(detailed ? sortedPartitions : null)
                      .metrics(metrics)
                      .build());
            });

    return ConsumerGroupLagSnapshot.builder()
        .consumerGroupId(consumerGroupId)
        .topics(topics)
        .build();
  }

  @Nullable
  public static LagMetricsSnapshot lagMetricsFromPartitionLags(@Nullable List<Long> lags) {
    return calculateLagMetrics(lags);
  }

  @Nullable
  private static LagMetricsSnapshot calculateLagMetrics(@Nullable List<Long> lags) {
    if (lags == null || lags.isEmpty()) {
      return null;
    }
    List<Long> sortedLags = new ArrayList<>(lags);
    Collections.sort(sortedLags);
    long maxLag = sortedLags.get(sortedLags.size() - 1);
    int middle = sortedLags.size() / 2;
    long medianLag;
    if (sortedLags.size() % 2 == 0) {
      medianLag = (sortedLags.get(middle - 1) + sortedLags.get(middle)) / 2;
    } else {
      medianLag = sortedLags.get(middle);
    }
    long totalLag = lags.stream().mapToLong(Long::longValue).sum();
    long avgLag = Math.round((double) totalLag / lags.size());
    return LagMetricsSnapshot.builder()
        .maxLag(maxLag)
        .medianLag(medianLag)
        .totalLag(totalLag)
        .avgLag(avgLag)
        .build();
  }
}
