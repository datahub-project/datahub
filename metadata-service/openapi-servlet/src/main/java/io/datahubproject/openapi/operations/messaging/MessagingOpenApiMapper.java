package io.datahubproject.openapi.operations.messaging;

import com.linkedin.metadata.messaging.ConsumerGroupLagSnapshot;
import com.linkedin.metadata.messaging.PartitionLagSnapshot;
import com.linkedin.metadata.messaging.TopicLagSnapshot;
import io.datahubproject.openapi.operations.kafka.KafkaOffsetResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Maps {@link ConsumerGroupLagSnapshot} to {@link KafkaOffsetResponse} for OpenAPI serialization.
 */
public final class MessagingOpenApiMapper {

  private MessagingOpenApiMapper() {}

  @Nonnull
  public static KafkaOffsetResponse toKafkaOffsetResponse(
      @Nonnull ConsumerGroupLagSnapshot snapshot) {
    KafkaOffsetResponse response = new KafkaOffsetResponse();
    Map<String, KafkaOffsetResponse.TopicOffsetInfo> topicMap = new LinkedHashMap<>();
    for (Map.Entry<String, TopicLagSnapshot> topicEntry : snapshot.getTopics().entrySet()) {
      TopicLagSnapshot tls = topicEntry.getValue();
      Map<String, KafkaOffsetResponse.PartitionInfo> partitionInfos = new LinkedHashMap<>();
      if (tls.getPartitions() != null) {
        for (Map.Entry<String, PartitionLagSnapshot> pe : tls.getPartitions().entrySet()) {
          PartitionLagSnapshot pl = pe.getValue();
          partitionInfos.put(
              pe.getKey(),
              KafkaOffsetResponse.PartitionInfo.builder()
                  .offset(pl.getOffset())
                  .lag(pl.getLag())
                  .aheadBy(pl.getAheadBy())
                  .metadata(pl.getMetadata())
                  .build());
        }
      }
      KafkaOffsetResponse.LagMetrics metrics =
          tls.getMetrics()
              .map(
                  lm ->
                      KafkaOffsetResponse.LagMetrics.builder()
                          .maxLag(lm.getMaxLag())
                          .medianLag(lm.getMedianLag())
                          .totalLag(lm.getTotalLag())
                          .avgLag(lm.getAvgLag())
                          .build())
              .orElse(null);
      topicMap.put(
          topicEntry.getKey(),
          KafkaOffsetResponse.TopicOffsetInfo.builder()
              .partitions(tls.getPartitions() == null ? null : partitionInfos)
              .metrics(metrics)
              .build());
    }
    response.put(snapshot.getConsumerGroupId(), topicMap);
    return response;
  }

  @Nonnull
  public static MessagingLagEnvelope toEnvelope(
      @Nonnull String transport, @Nonnull ConsumerGroupLagSnapshot snapshot) {
    return new MessagingLagEnvelope(transport, toKafkaOffsetResponse(snapshot));
  }
}
