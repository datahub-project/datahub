package com.linkedin.metadata.messaging;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;

public class ConsumerLagSnapshotMapperTest {

  @Test
  public void testNullConsumerGroupReturnsEmptySnapshot() {
    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets(
            null, Collections.emptyMap(), Collections.emptyMap(), false);
    assertNotNull(snapshot);
    assertEquals(snapshot.getConsumerGroupId(), "");
    assertTrue(snapshot.getTopics().isEmpty());
  }

  @Test
  public void testEmptyOffsetsReturnsEmptySnapshot() {
    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets(
            "my-group", Collections.emptyMap(), Collections.emptyMap(), false);
    assertNotNull(snapshot);
    assertEquals(snapshot.getConsumerGroupId(), "my-group");
    assertTrue(snapshot.getTopics().isEmpty());
  }

  @Test
  public void testSinglePartitionLag() {
    TopicPartition tp = new TopicPartition("topic-a", 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, new OffsetAndMetadata(50));
    Map<TopicPartition, Long> endOffsets = Map.of(tp, 100L);

    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets("grp", offsets, endOffsets, true);

    assertEquals(snapshot.getConsumerGroupId(), "grp");
    assertEquals(snapshot.getTopics().size(), 1);
    TopicLagSnapshot topicSnap = snapshot.getTopics().get("topic-a");
    assertNotNull(topicSnap);
    assertNotNull(topicSnap.getPartitions());
    assertEquals(topicSnap.getPartitions().size(), 1);
    PartitionLagSnapshot part = topicSnap.getPartitions().get("0");
    assertEquals(part.getOffset(), 50L);
    assertEquals(part.getLag().longValue(), 50L);

    LagMetricsSnapshot metrics = topicSnap.getMetrics().orElse(null);
    assertNotNull(metrics);
    assertEquals(metrics.getMaxLag(), 50L);
    assertEquals(metrics.getMedianLag(), 50L);
    assertEquals(metrics.getTotalLag(), 50L);
    assertEquals(metrics.getAvgLag(), 50L);
  }

  @Test
  public void testMultiPartitionMedianOddCount() {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    // 3 partitions with lags: 10, 30, 50
    offsets.put(new TopicPartition("t", 0), new OffsetAndMetadata(90));
    endOffsets.put(new TopicPartition("t", 0), 100L); // lag=10
    offsets.put(new TopicPartition("t", 1), new OffsetAndMetadata(70));
    endOffsets.put(new TopicPartition("t", 1), 100L); // lag=30
    offsets.put(new TopicPartition("t", 2), new OffsetAndMetadata(50));
    endOffsets.put(new TopicPartition("t", 2), 100L); // lag=50

    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets("grp", offsets, endOffsets, false);

    LagMetricsSnapshot metrics = snapshot.getTopics().get("t").getMetrics().orElse(null);
    assertNotNull(metrics);
    assertEquals(metrics.getMaxLag(), 50L);
    assertEquals(metrics.getMedianLag(), 30L); // sorted=[10,30,50], middle=30
    assertEquals(metrics.getTotalLag(), 90L);
    assertEquals(metrics.getAvgLag(), 30L);
  }

  @Test
  public void testMultiPartitionMedianEvenCount() {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    // 4 partitions with lags: 10, 20, 30, 40
    offsets.put(new TopicPartition("t", 0), new OffsetAndMetadata(90));
    endOffsets.put(new TopicPartition("t", 0), 100L); // lag=10
    offsets.put(new TopicPartition("t", 1), new OffsetAndMetadata(80));
    endOffsets.put(new TopicPartition("t", 1), 100L); // lag=20
    offsets.put(new TopicPartition("t", 2), new OffsetAndMetadata(70));
    endOffsets.put(new TopicPartition("t", 2), 100L); // lag=30
    offsets.put(new TopicPartition("t", 3), new OffsetAndMetadata(60));
    endOffsets.put(new TopicPartition("t", 3), 100L); // lag=40

    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets("grp", offsets, endOffsets, false);

    LagMetricsSnapshot metrics = snapshot.getTopics().get("t").getMetrics().orElse(null);
    assertNotNull(metrics);
    assertEquals(metrics.getMaxLag(), 40L);
    assertEquals(metrics.getMedianLag(), 25L); // sorted=[10,20,30,40], avg(20,30)=25
    assertEquals(metrics.getTotalLag(), 100L);
    assertEquals(metrics.getAvgLag(), 25L);
  }

  @Test
  public void testMissingEndOffsetLagIsNull() {
    TopicPartition tp = new TopicPartition("topic-a", 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, new OffsetAndMetadata(50));
    Map<TopicPartition, Long> endOffsets = Collections.emptyMap();

    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets("grp", offsets, endOffsets, true);

    TopicLagSnapshot topicSnap = snapshot.getTopics().get("topic-a");
    assertNotNull(topicSnap);
    PartitionLagSnapshot part = topicSnap.getPartitions().get("0");
    assertNull(part.getLag());
    assertTrue(topicSnap.getMetrics().isEmpty());
  }

  @Test
  public void testDetailedFalseOmitsPartitions() {
    TopicPartition tp = new TopicPartition("topic-a", 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, new OffsetAndMetadata(50));
    Map<TopicPartition, Long> endOffsets = Map.of(tp, 100L);

    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets("grp", offsets, endOffsets, false);

    TopicLagSnapshot topicSnap = snapshot.getTopics().get("topic-a");
    assertNotNull(topicSnap);
    assertNull(topicSnap.getPartitions());
    assertTrue(topicSnap.getMetrics().isPresent());
  }

  @Test
  public void testTopicsAreSortedAlphabetically() {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    offsets.put(new TopicPartition("zebra", 0), new OffsetAndMetadata(0));
    endOffsets.put(new TopicPartition("zebra", 0), 10L);
    offsets.put(new TopicPartition("apple", 0), new OffsetAndMetadata(0));
    endOffsets.put(new TopicPartition("apple", 0), 5L);

    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets("grp", offsets, endOffsets, false);

    List<String> keys = List.copyOf(snapshot.getTopics().keySet());
    assertEquals(keys.get(0), "apple");
    assertEquals(keys.get(1), "zebra");
  }

  @Test
  public void testPartitionsAreSortedNumerically() {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    offsets.put(new TopicPartition("t", 2), new OffsetAndMetadata(0));
    endOffsets.put(new TopicPartition("t", 2), 10L);
    offsets.put(new TopicPartition("t", 0), new OffsetAndMetadata(0));
    endOffsets.put(new TopicPartition("t", 0), 10L);
    offsets.put(new TopicPartition("t", 1), new OffsetAndMetadata(0));
    endOffsets.put(new TopicPartition("t", 1), 10L);

    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets("grp", offsets, endOffsets, true);

    List<String> keys = List.copyOf(snapshot.getTopics().get("t").getPartitions().keySet());
    assertEquals(keys, List.of("0", "1", "2"));
  }

  @Test
  public void testLagMetricsFromPartitionLagsNullReturnsNull() {
    assertNull(ConsumerLagSnapshotMapper.lagMetricsFromPartitionLags(null));
  }

  @Test
  public void testLagMetricsFromPartitionLagsEmptyReturnsNull() {
    assertNull(ConsumerLagSnapshotMapper.lagMetricsFromPartitionLags(Collections.emptyList()));
  }

  @Test
  public void testLagMetricsFromPartitionLagsSingleElement() {
    LagMetricsSnapshot metrics =
        ConsumerLagSnapshotMapper.lagMetricsFromPartitionLags(List.of(42L));
    assertNotNull(metrics);
    assertEquals(metrics.getMaxLag(), 42L);
    assertEquals(metrics.getMedianLag(), 42L);
    assertEquals(metrics.getTotalLag(), 42L);
    assertEquals(metrics.getAvgLag(), 42L);
  }

  @Test
  public void testNegativeLagClampedToZero() {
    TopicPartition tp = new TopicPartition("t", 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(tp, new OffsetAndMetadata(200));
    Map<TopicPartition, Long> endOffsets = Map.of(tp, 100L);

    ConsumerGroupLagSnapshot snapshot =
        ConsumerLagSnapshotMapper.fromKafkaOffsets("grp", offsets, endOffsets, true);

    PartitionLagSnapshot part = snapshot.getTopics().get("t").getPartitions().get("0");
    assertEquals(part.getLag().longValue(), 0L);
  }
}
