package com.linkedin.gms.factory.messaging;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.messaging.ConsumerGroupLagSnapshot;
import com.linkedin.metadata.messaging.PartitionLagSnapshot;
import com.linkedin.metadata.messaging.TopicLagSnapshot;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.mxe.TopicConvention;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueueConsumerLagPortTest {

  private static final String TOPIC = "MetadataChangeLog_Versioned_v1";
  private static final String GROUP = "generic-mae-consumer-job-client";

  private MetadataQueueStore store;
  private TopicConvention topicConvention;
  private PgQueueConsumerLagPort port;

  @BeforeMethod
  public void setUp() {
    store = mock(MetadataQueueStore.class);
    topicConvention = mock(TopicConvention.class);
    when(topicConvention.getMetadataChangeLogVersionedTopicName()).thenReturn(TOPIC);
    port = new PgQueueConsumerLagPort(store, topicConvention);
    try {
      var field = PgQueueConsumerLagPort.class.getDeclaredField("maeConsumerGroupId");
      field.setAccessible(true);
      field.set(port, GROUP);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void mclVersionedLag_reportsAheadByPerPartition() {
    long topicId = 7L;
    int partitions = 2;
    when(store.fetchTopic(TOPIC))
        .thenReturn(Optional.of(new QueueTopicMetadata(topicId, partitions, Optional.empty())));
    when(store.partitionMaxEnqueueSeqs(topicId, partitions)).thenReturn(Map.of(0, 5L, 1, 0L));
    when(store.getCommittedOffset(GROUP, topicId, 0)).thenReturn(10L);
    when(store.getCommittedOffset(GROUP, topicId, 1)).thenReturn(0L);

    ConsumerGroupLagSnapshot snap = port.mclVersionedLag(false, true);
    TopicLagSnapshot topic = snap.getTopics().get(TOPIC);
    assertNotNull(topic);
    PartitionLagSnapshot p0 = topic.getPartitions().get("0");
    assertEquals(p0.getAheadBy().longValue(), 5L);
    assertEquals(p0.getMetadata(), "STUCK_AHEAD");
    PartitionLagSnapshot p1 = topic.getPartitions().get("1");
    assertEquals(p1.getAheadBy(), null);
  }
}
