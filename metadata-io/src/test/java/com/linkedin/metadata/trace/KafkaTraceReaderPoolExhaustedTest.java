package com.linkedin.metadata.trace;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaTraceReaderPoolExhaustedTest {

  private static final String TOPIC_NAME = "test-topic";
  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:container:123");

  @Mock private AdminClient adminClient;
  @Mock private Consumer<String, GenericRecord> consumer;

  private ExecutorService executorService;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
    executorService = Executors.newFixedThreadPool(2);
    setupTopicDescriptionMock();
  }

  private void setupTopicDescriptionMock() {
    Node mockNode = new Node(0, "localhost", 9092);
    TopicPartitionInfo partitionInfo =
        new TopicPartitionInfo(
            0, mockNode, Collections.singletonList(mockNode), Collections.singletonList(mockNode));
    TopicDescription topicDescription =
        new TopicDescription(TOPIC_NAME, false, Collections.singletonList(partitionInfo));
    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    when(describeTopicsResult.topicNameValues())
        .thenReturn(Map.of(TOPIC_NAME, KafkaFuture.completedFuture(topicDescription)));
    when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (executorService != null) {
      executorService.shutdownNow();
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    }
    mocks.close();
  }

  @Test
  public void testFindMessages_PropagatesPoolExhaustedException() {
    TraceConsumerPool exhaustedPool =
        new TraceConsumerPool() {
          @Override
          public <T> T withConsumer(String topic, TraceConsumerAction<T> action) {
            throw new TraceConsumerPoolExhaustedException("pool exhausted");
          }

          @Override
          public void shutdown() {}
        };

    MCPTraceReader reader =
        MCPTraceReader.builder()
            .adminClient(adminClient)
            .topicName(TOPIC_NAME)
            .consumerGroupId("test-group")
            .consumerPool(exhaustedPool)
            .pollDurationMs(100)
            .pollMaxAttempts(1)
            .executorService(executorService)
            .timeoutSeconds(5)
            .cancelFuturesOnTimeout(true)
            .build();

    TraceConsumerPoolExhaustedException exception =
        expectThrows(
            TraceConsumerPoolExhaustedException.class,
            () ->
                reader.findMessages(
                    Map.of(TEST_URN, List.of("status")), "trace-id", System.currentTimeMillis()));

    assertTrue(exception.getMessage().contains("pool exhausted"));
  }
}
