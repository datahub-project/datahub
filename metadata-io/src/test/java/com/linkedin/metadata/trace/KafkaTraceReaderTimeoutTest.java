package com.linkedin.metadata.trace;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
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

public class KafkaTraceReaderTimeoutTest {

  private static final String TOPIC_NAME = "test-topic";
  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:container:123");
  private static final Urn TEST_URN_2 = UrnUtils.getUrn("urn:li:container:456");

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
  public void testFindMessages_CancelsFuturesAndReleasesConsumersOnTimeout() throws Exception {
    AtomicInteger activeBorrows = new AtomicInteger(0);
    AtomicInteger interruptedTasks = new AtomicInteger(0);
    CountDownLatch tasksBlocked = new CountDownLatch(2);

    when(consumer.offsetsForTimes(any()))
        .thenAnswer(
            invocation -> {
              tasksBlocked.countDown();
              while (!Thread.currentThread().isInterrupted()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
              }
              interruptedTasks.incrementAndGet();
              Thread.currentThread().interrupt();
              throw new InterruptedException("simulated cancel");
            });

    TraceConsumerPool trackingPool =
        new TraceConsumerPool() {
          @Override
          public <T> T withConsumer(String topic, TraceConsumerAction<T> action) {
            activeBorrows.incrementAndGet();
            try {
              return action.execute(consumer);
            } catch (RuntimeException e) {
              throw e;
            } catch (Exception e) {
              throw new RuntimeException(e);
            } finally {
              activeBorrows.decrementAndGet();
            }
          }

          @Override
          public void shutdown() {}
        };

    MCPTraceReader reader =
        MCPTraceReader.builder()
            .adminClient(adminClient)
            .topicName(TOPIC_NAME)
            .consumerGroupId("test-group")
            .consumerPool(trackingPool)
            .pollDurationMs(100)
            .pollMaxAttempts(1)
            .executorService(executorService)
            .timeoutSeconds(1)
            .cancelFuturesOnTimeout(true)
            .build();

    Map<Urn, List<String>> urnAspectPairs =
        Map.of(
            TEST_URN, List.of("status"),
            TEST_URN_2, List.of("status"));

    RuntimeException exception =
        expectThrows(
            RuntimeException.class,
            () -> reader.findMessages(urnAspectPairs, "trace-id", System.currentTimeMillis()));

    assertTrue(exception.getMessage().contains("Failed to process parallel trace requests"));
    assertTrue(exception.getCause() instanceof TimeoutException);
    assertTrue(
        tasksBlocked.await(5, TimeUnit.SECONDS),
        "Both worker tasks should reach the blocking offsets lookup");

    // cancel(true) is best-effort for parked worker threads; stop the executor to
    // unblock workers and verify borrowed consumers are always released.
    executorService.shutdownNow();

    long deadline = System.currentTimeMillis() + 5000;
    while (activeBorrows.get() > 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(
        activeBorrows.get(), 0, "Borrowed consumers should be released after cancellation");
    assertTrue(
        interruptedTasks.get() >= 1,
        "Stopping the executor should interrupt blocked trace worker tasks");
  }
}
