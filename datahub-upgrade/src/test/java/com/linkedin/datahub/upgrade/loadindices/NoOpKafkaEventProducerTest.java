package com.linkedin.datahub.upgrade.loadindices;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import java.util.concurrent.Future;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NoOpKafkaEventProducerTest {

  private NoOpKafkaEventProducer producer;
  private Urn testUrn;
  private AspectSpec mockAspectSpec;

  @BeforeMethod
  public void setUp() {
    producer = new NoOpKafkaEventProducer();
    testUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    mockAspectSpec = mock(AspectSpec.class);
  }

  @Test
  public void testConstructor() {
    assertNotNull(producer);
  }

  @Test
  public void testProduceMetadataChangeLog() throws Exception {
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(testUrn);

    Future<?> result = producer.produceMetadataChangeLog(testUrn, mockAspectSpec, mcl);

    assertNotNull(result);
    assertTrue(result.isDone());
    assertNull(result.get()); // Should complete immediately with null
  }

  @Test
  public void testProduceMetadataChangeLogWithNullAspectSpec() throws Exception {
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(testUrn);

    Future<?> result = producer.produceMetadataChangeLog(testUrn, null, mcl);

    assertNotNull(result);
    assertTrue(result.isDone());
    assertNull(result.get());
  }

  @Test
  public void testGetMetadataChangeLogTopicName() {
    String topicName = producer.getMetadataChangeLogTopicName(mockAspectSpec);

    assertEquals(topicName, "no-op-mcl-topic");
  }

  @Test
  public void testProduceMetadataChangeProposal() throws Exception {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(testUrn);

    Future<?> result = producer.produceMetadataChangeProposal(testUrn, mcp);

    assertNotNull(result);
    assertTrue(result.isDone());
    assertNull(result.get());
  }

  @Test
  public void testGetMetadataChangeProposalTopicName() {
    String topicName = producer.getMetadataChangeProposalTopicName();

    assertEquals(topicName, "no-op-mcp-topic");
  }

  @Test
  public void testProduceFailedMetadataChangeProposalAsync() throws Exception {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(testUrn);

    OperationContext mockOpContext = mock(OperationContext.class);
    Set<Throwable> throwables = Set.of(new RuntimeException("Test error"));

    Future<?> result =
        producer.produceFailedMetadataChangeProposalAsync(mockOpContext, mcp, throwables);

    assertNotNull(result);
    assertTrue(result.isDone());
    assertNull(result.get());
  }

  @Test
  public void testProducePlatformEvent() throws Exception {
    PlatformEvent platformEvent = new PlatformEvent();
    String eventName = "test-event";
    String key = "test-key";

    Future<?> result = producer.producePlatformEvent(eventName, key, platformEvent);

    assertNotNull(result);
    assertTrue(result.isDone());
    assertNull(result.get());
  }

  @Test
  public void testProducePlatformEventWithNullKey() throws Exception {
    PlatformEvent platformEvent = new PlatformEvent();
    String eventName = "test-event";

    Future<?> result = producer.producePlatformEvent(eventName, null, platformEvent);

    assertNotNull(result);
    assertTrue(result.isDone());
    assertNull(result.get());
  }

  @Test
  public void testGetPlatformEventTopicName() {
    String topicName = producer.getPlatformEventTopicName();

    assertEquals(topicName, "no-op-platform-topic");
  }

  @Test
  public void testProduceDataHubUpgradeHistoryEvent() {
    DataHubUpgradeHistoryEvent event = new DataHubUpgradeHistoryEvent();

    // Should not throw exception
    producer.produceDataHubUpgradeHistoryEvent(event);
  }

  @Test
  public void testFlush() {
    // Should not throw exception
    producer.flush();
  }

  @Test
  public void testMultipleCalls() {
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(testUrn);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(testUrn);

    PlatformEvent platformEvent = new PlatformEvent();

    // Multiple calls should all work
    Future<?> result1 = producer.produceMetadataChangeLog(testUrn, mockAspectSpec, mcl);
    Future<?> result2 = producer.produceMetadataChangeProposal(testUrn, mcp);
    Future<?> result3 = producer.producePlatformEvent("test", "key", platformEvent);

    assertNotNull(result1);
    assertNotNull(result2);
    assertNotNull(result3);

    assertTrue(result1.isDone());
    assertTrue(result2.isDone());
    assertTrue(result3.isDone());
  }

  @Test
  public void testConcurrentCalls() throws Exception {
    int numThreads = 10;
    Thread[] threads = new Thread[numThreads];
    Future<?>[] results = new Future[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      threads[i] =
          new Thread(
              () -> {
                MetadataChangeLog mcl = new MetadataChangeLog();
                mcl.setEntityUrn(testUrn);
                results[threadId] = producer.produceMetadataChangeLog(testUrn, mockAspectSpec, mcl);
              });
    }

    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // Verify all results
    for (Future<?> result : results) {
      assertNotNull(result);
      assertTrue(result.isDone());
      assertNull(result.get());
    }
  }

  @Test
  public void testProducerMethods() {
    // Test that the internal producer methods work without throwing exceptions
    producer.flush();

    // These methods should not throw exceptions
    try {
      producer.flush();
    } catch (Exception e) {
      fail("flush() should not throw exceptions");
    }
  }

  @Test
  public void testTopicNameConsistency() {
    // Verify topic names are consistent
    assertEquals(producer.getMetadataChangeLogTopicName(mockAspectSpec), "no-op-mcl-topic");
    assertEquals(producer.getMetadataChangeProposalTopicName(), "no-op-mcp-topic");
    assertEquals(producer.getPlatformEventTopicName(), "no-op-platform-topic");
  }

  @Test
  public void testNullHandling() {
    // Test with null inputs
    try {
      producer.produceMetadataChangeLog(testUrn, null, null);
      producer.produceMetadataChangeProposal(testUrn, null);
      producer.producePlatformEvent(null, null, null);
      producer.produceDataHubUpgradeHistoryEvent(null);
    } catch (Exception e) {
      fail("Methods should handle null inputs gracefully: " + e.getMessage());
    }
  }
}
