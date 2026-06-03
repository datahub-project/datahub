package com.linkedin.gms.factory.messaging;

import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.messaging.ConsumerGroupLagSnapshot;
import com.linkedin.metadata.messaging.ConsumerLagPort;
import com.linkedin.metadata.messaging.ConsumerLagSnapshotMapper;
import com.linkedin.metadata.trace.MCLTraceReader;
import com.linkedin.metadata.trace.MCPTraceReader;
import java.util.Map;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@KafkaMessagingEnabled
public class KafkaConsumerLagPort implements ConsumerLagPort {

  private final MCPTraceReader mcpTraceReader;
  private final MCLTraceReader mclVersionedTraceReader;
  private final MCLTraceReader mclTimeseriesTraceReader;

  public KafkaConsumerLagPort(
      MCPTraceReader mcpTraceReader,
      @Qualifier("mclVersionedTraceReader") MCLTraceReader mclVersionedTraceReader,
      @Qualifier("mclTimeseriesTraceReader") MCLTraceReader mclTimeseriesTraceReader) {
    this.mcpTraceReader = mcpTraceReader;
    this.mclVersionedTraceReader = mclVersionedTraceReader;
    this.mclTimeseriesTraceReader = mclTimeseriesTraceReader;
  }

  @Override
  @Nonnull
  public String transport() {
    return "kafka";
  }

  @Override
  @Nonnull
  public ConsumerGroupLagSnapshot mcpLag(boolean skipCache, boolean detailed) {
    var offsetMap = mcpTraceReader.getAllPartitionOffsets(skipCache);
    var endOffsets = mcpTraceReader.getEndOffsets(offsetMap.keySet(), skipCache);
    return ConsumerLagSnapshotMapper.fromKafkaOffsets(
        mcpTraceReader.getConsumerGroupId(), offsetMap, endOffsets, detailed);
  }

  @Override
  @Nonnull
  public ConsumerGroupLagSnapshot mclVersionedLag(boolean skipCache, boolean detailed) {
    var offsetMap = mclVersionedTraceReader.getAllPartitionOffsets(skipCache);
    var endOffsets = mclVersionedTraceReader.getEndOffsets(offsetMap.keySet(), skipCache);
    return ConsumerLagSnapshotMapper.fromKafkaOffsets(
        mclVersionedTraceReader.getConsumerGroupId(), offsetMap, endOffsets, detailed);
  }

  @Override
  @Nonnull
  public ConsumerGroupLagSnapshot mclTimeseriesLag(boolean skipCache, boolean detailed) {
    var offsetMap = mclTimeseriesTraceReader.getAllPartitionOffsets(skipCache);
    var endOffsets = mclTimeseriesTraceReader.getEndOffsets(offsetMap.keySet(), skipCache);
    return ConsumerLagSnapshotMapper.fromKafkaOffsets(
        mclTimeseriesTraceReader.getConsumerGroupId(), offsetMap, endOffsets, detailed);
  }

  @Override
  @Nonnull
  public ConsumerGroupLagSnapshot usageEventsLag(boolean skipCache, boolean detailed) {
    // Usage events use a dedicated Spring Kafka listener; lag is not exposed via trace readers.
    return ConsumerGroupLagSnapshot.builder()
        .consumerGroupId("datahub-usage-event-consumer-job-client")
        .topics(Map.of())
        .build();
  }
}
