package io.datahubproject.openapi.operations.messaging;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.messaging.ConsumerGroupLagSnapshot;
import com.linkedin.metadata.messaging.PartitionLagSnapshot;
import com.linkedin.metadata.messaging.TopicLagSnapshot;
import io.datahubproject.openapi.operations.kafka.KafkaOffsetResponse;
import java.util.Map;
import org.testng.annotations.Test;

public class MessagingOpenApiMapperTest {

  @Test
  public void toKafkaOffsetResponse_mapsPartitionsAndMetrics() {
    ConsumerGroupLagSnapshot snapshot =
        ConsumerGroupLagSnapshot.builder()
            .consumerGroupId("g1")
            .topics(
                Map.of(
                    "topic-a",
                    TopicLagSnapshot.builder()
                        .partitions(
                            Map.of(
                                "0",
                                PartitionLagSnapshot.builder()
                                    .offset(10L)
                                    .lag(2L)
                                    .aheadBy(5L)
                                    .metadata("STUCK_AHEAD")
                                    .build()))
                        .build()))
            .build();

    KafkaOffsetResponse response = MessagingOpenApiMapper.toKafkaOffsetResponse(snapshot);
    @SuppressWarnings("unchecked")
    Map<String, KafkaOffsetResponse.TopicOffsetInfo> topics =
        (Map<String, KafkaOffsetResponse.TopicOffsetInfo>) response.get("g1");
    assertNotNull(topics);
    assertEquals(topics.get("topic-a").getPartitions().get("0").getAheadBy().longValue(), 5L);
  }

  @Test
  public void toEnvelope_includesTransport() {
    ConsumerGroupLagSnapshot snapshot =
        ConsumerGroupLagSnapshot.builder().consumerGroupId("g1").topics(Map.of()).build();

    MessagingLagEnvelope envelope = MessagingOpenApiMapper.toEnvelope("pgqueue", snapshot);
    assertEquals(envelope.getTransport(), "pgqueue");
    assertNotNull(envelope.getConsumerGroups());
  }
}
