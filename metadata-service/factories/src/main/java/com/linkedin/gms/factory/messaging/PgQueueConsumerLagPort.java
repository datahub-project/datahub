package com.linkedin.gms.factory.messaging;

import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.messaging.ConsumerGroupLagSnapshot;
import com.linkedin.metadata.messaging.ConsumerLagPort;
import com.linkedin.metadata.messaging.ConsumerLagSnapshotMapper;
import com.linkedin.metadata.messaging.LagMetricsSnapshot;
import com.linkedin.metadata.messaging.PartitionLagSnapshot;
import com.linkedin.metadata.messaging.TopicLagSnapshot;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PartitionOffsetSkew;
import com.linkedin.metadata.queue.PgQueueOffsetSkewWarnings;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.mxe.ConsumerGroups;
import com.linkedin.mxe.TopicConvention;
import com.linkedin.mxe.Topics;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

@Component
@Conditional(PgQueueMessagingTransportCondition.class)
public class PgQueueConsumerLagPort implements ConsumerLagPort {

  private static final long STUCK_AHEAD_EFFECTIVE_LAG = Long.MAX_VALUE / 4;

  private final MetadataQueueStore metadataQueueStore;
  private final TopicConvention topicConvention;

  @Value(ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE)
  private String mceConsumerGroupId;

  @Value("${METADATA_CHANGE_LOG_KAFKA_CONSUMER_GROUP_ID:generic-mae-consumer-job-client}")
  private String maeConsumerGroupId;

  @Value("${DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_ID:datahub-usage-event-consumer-job-client}")
  private String usageEventConsumerGroupId;

  @Value("${DATAHUB_USAGE_EVENT_NAME:" + Topics.DATAHUB_USAGE_EVENT + "}")
  private String usageEventTopicName;

  public PgQueueConsumerLagPort(
      MetadataQueueStore metadataQueueStore,
      @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN) TopicConvention topicConvention) {
    this.metadataQueueStore = metadataQueueStore;
    this.topicConvention = topicConvention;
  }

  @Override
  @Nonnull
  public String transport() {
    return "pgqueue";
  }

  @Override
  @Nonnull
  public ConsumerGroupLagSnapshot mcpLag(boolean skipCache, boolean detailed) {
    return lagForTopic(
        topicConvention.getMetadataChangeProposalTopicName(), mceConsumerGroupId, detailed);
  }

  @Override
  @Nonnull
  public ConsumerGroupLagSnapshot mclVersionedLag(boolean skipCache, boolean detailed) {
    return lagForTopic(
        topicConvention.getMetadataChangeLogVersionedTopicName(), maeConsumerGroupId, detailed);
  }

  @Override
  @Nonnull
  public ConsumerGroupLagSnapshot mclTimeseriesLag(boolean skipCache, boolean detailed) {
    return lagForTopic(
        topicConvention.getMetadataChangeLogTimeseriesTopicName(), maeConsumerGroupId, detailed);
  }

  @Override
  @Nonnull
  public ConsumerGroupLagSnapshot usageEventsLag(boolean skipCache, boolean detailed) {
    return lagForTopic(usageEventTopicName, usageEventConsumerGroupId, detailed);
  }

  @Nonnull
  private ConsumerGroupLagSnapshot lagForTopic(
      @Nonnull String topicName, @Nonnull String consumerGroupId, boolean detailed) {
    Optional<QueueTopicMetadata> meta = metadataQueueStore.fetchTopic(topicName);
    if (meta.isEmpty()) {
      return ConsumerGroupLagSnapshot.builder()
          .consumerGroupId(consumerGroupId)
          .topics(Map.of())
          .build();
    }
    long topicId = meta.get().id();
    int partitionCount = meta.get().partitionCount();
    Map<Integer, Long> maxSeqs =
        metadataQueueStore.partitionMaxEnqueueSeqs(topicId, partitionCount);
    Map<String, PartitionLagSnapshot> partitions = new LinkedHashMap<>();
    List<Long> lags = new ArrayList<>();
    for (int p = 0; p < partitionCount; p++) {
      long lastSeq = maxSeqs.getOrDefault(p, 0L);
      long committed = metadataQueueStore.getCommittedOffset(consumerGroupId, topicId, p);
      long lag = Math.max(0L, lastSeq - committed);
      long aheadBy = Math.max(0L, committed - lastSeq);
      if (aheadBy > 0) {
        PgQueueOffsetSkewWarnings.getInstance()
            .warnIfAhead(
                PartitionOffsetSkew.builder()
                    .consumerGroup(consumerGroupId)
                    .topicId(topicId)
                    .topicName(topicName)
                    .partitionId(p)
                    .committedOffset(committed)
                    .maxSeq(lastSeq)
                    .aheadBy(aheadBy)
                    .build());
        lags.add(STUCK_AHEAD_EFFECTIVE_LAG);
      } else {
        lags.add(lag);
      }
      partitions.put(
          String.valueOf(p),
          PartitionLagSnapshot.builder()
              .offset(committed)
              .lag(lag)
              .aheadBy(aheadBy > 0 ? aheadBy : null)
              .metadata(aheadBy > 0 ? "STUCK_AHEAD" : null)
              .build());
    }
    LagMetricsSnapshot metrics = ConsumerLagSnapshotMapper.lagMetricsFromPartitionLags(lags);
    TopicLagSnapshot topicSnap =
        TopicLagSnapshot.builder()
            .partitions(detailed ? partitions : null)
            .metrics(Optional.ofNullable(metrics))
            .build();
    return ConsumerGroupLagSnapshot.builder()
        .consumerGroupId(consumerGroupId)
        .topics(Map.of(topicName, topicSnap))
        .build();
  }
}
