package com.linkedin.gms.factory.messaging;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.messaging.ConsumerGroupLagSnapshot;
import com.linkedin.metadata.trace.MCLTraceReader;
import com.linkedin.metadata.trace.MCPTraceReader;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaConsumerLagPortTest {

  private MCPTraceReader mcpTraceReader;
  private MCLTraceReader mclVersionedTraceReader;
  private MCLTraceReader mclTimeseriesTraceReader;
  private KafkaConsumerLagPort port;

  @BeforeMethod
  public void setUp() {
    mcpTraceReader = mock(MCPTraceReader.class);
    mclVersionedTraceReader = mock(MCLTraceReader.class);
    mclTimeseriesTraceReader = mock(MCLTraceReader.class);
    port =
        new KafkaConsumerLagPort(mcpTraceReader, mclVersionedTraceReader, mclTimeseriesTraceReader);
  }

  @Test
  public void transportIsKafka() {
    assertEquals(port.transport(), "kafka");
  }

  @Test
  public void mcpLag_mapsOffsetsFromTraceReader() {
    TopicPartition tp = new TopicPartition("MetadataChangeProposal_v1", 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, new OffsetAndMetadata(40L));
    when(mcpTraceReader.getConsumerGroupId()).thenReturn("mcp-group");
    when(mcpTraceReader.getAllPartitionOffsets(true)).thenReturn(offsets);
    when(mcpTraceReader.getEndOffsets(eq(offsets.keySet()), eq(true))).thenReturn(Map.of(tp, 100L));

    ConsumerGroupLagSnapshot snapshot = port.mcpLag(true, true);

    assertEquals(snapshot.getConsumerGroupId(), "mcp-group");
    assertEquals(
        snapshot.getTopics().get("MetadataChangeProposal_v1").getPartitions().get("0").getLag(),
        Long.valueOf(60L));
    verify(mcpTraceReader).getAllPartitionOffsets(true);
    verify(mcpTraceReader).getEndOffsets(offsets.keySet(), true);
  }

  @Test
  public void mclVersionedLag_mapsOffsetsFromTraceReader() {
    TopicPartition tp = new TopicPartition("MetadataChangeLog_Versioned_v1", 1);
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, new OffsetAndMetadata(5L));
    when(mclVersionedTraceReader.getConsumerGroupId()).thenReturn("mae-group");
    when(mclVersionedTraceReader.getAllPartitionOffsets(false)).thenReturn(offsets);
    when(mclVersionedTraceReader.getEndOffsets(eq(offsets.keySet()), eq(false)))
        .thenReturn(Map.of(tp, 8L));

    ConsumerGroupLagSnapshot snapshot = port.mclVersionedLag(false, false);

    assertEquals(snapshot.getConsumerGroupId(), "mae-group");
    assertEquals(
        snapshot
            .getTopics()
            .get("MetadataChangeLog_Versioned_v1")
            .getMetrics()
            .orElseThrow()
            .getMaxLag(),
        3L);
  }

  @Test
  public void mclTimeseriesLag_mapsOffsetsFromTraceReader() {
    TopicPartition tp = new TopicPartition("MetadataChangeLog_Timeseries_v1", 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, new OffsetAndMetadata(1L));
    when(mclTimeseriesTraceReader.getConsumerGroupId()).thenReturn("mae-ts-group");
    when(mclTimeseriesTraceReader.getAllPartitionOffsets(false)).thenReturn(offsets);
    when(mclTimeseriesTraceReader.getEndOffsets(eq(offsets.keySet()), eq(false)))
        .thenReturn(Map.of(tp, 4L));

    ConsumerGroupLagSnapshot snapshot = port.mclTimeseriesLag(false, true);

    assertEquals(snapshot.getConsumerGroupId(), "mae-ts-group");
    assertEquals(
        snapshot
            .getTopics()
            .get("MetadataChangeLog_Timeseries_v1")
            .getPartitions()
            .get("0")
            .getLag(),
        Long.valueOf(3L));
  }

  @Test
  public void usageEventsLag_returnsEmptyTopics() {
    ConsumerGroupLagSnapshot snapshot = port.usageEventsLag(false, true);
    assertEquals(snapshot.getConsumerGroupId(), "datahub-usage-event-consumer-job-client");
    assertTrue(snapshot.getTopics().isEmpty());
  }
}
