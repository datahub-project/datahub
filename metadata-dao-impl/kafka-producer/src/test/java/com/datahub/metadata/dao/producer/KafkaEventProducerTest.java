package com.datahub.metadata.dao.producer;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.GenericPayload;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class KafkaEventProducerTest {

  private Producer<String, ? extends IndexedRecord> mockProducer;
  private TopicConvention mockTopicConvention;
  private KafkaHealthChecker mockHealthChecker;
  private MetricUtils mockMetricUtils;
  private KafkaEventProducer eventProducer;
  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  private static final String TEST_URN_STRING = "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)";
  private static final String MCL_TOPIC = "MetadataChangeLog_v1";
  private static final String MCL_TIMESERIES_TOPIC = "MetadataChangeLog_Timeseries_v1";
  private static final String MCP_TOPIC = "MetadataChangeProposal_v1";
  private static final String FMCP_TOPIC = "FailedMetadataChangeProposal_v1";
  private static final String PLATFORM_EVENT_TOPIC = "PlatformEvent_v1";
  private static final String UPGRADE_HISTORY_TOPIC = "DataHubUpgradeHistory_v1";

  @BeforeMethod
  public void setup() {
    mockProducer = mock(Producer.class);
    mockTopicConvention = mock(TopicConvention.class);
    mockHealthChecker = mock(KafkaHealthChecker.class);
    mockMetricUtils = mock(MetricUtils.class);

    // Setup default topic names
    when(mockTopicConvention.getMetadataChangeLogVersionedTopicName()).thenReturn(MCL_TOPIC);
    when(mockTopicConvention.getMetadataChangeLogTimeseriesTopicName())
        .thenReturn(MCL_TIMESERIES_TOPIC);
    when(mockTopicConvention.getMetadataChangeProposalTopicName()).thenReturn(MCP_TOPIC);
    when(mockTopicConvention.getFailedMetadataChangeProposalTopicName()).thenReturn(FMCP_TOPIC);
    when(mockTopicConvention.getPlatformEventTopicName()).thenReturn(PLATFORM_EVENT_TOPIC);
    when(mockTopicConvention.getDataHubUpgradeHistoryTopicName()).thenReturn(UPGRADE_HISTORY_TOPIC);

    // Setup health checker callback
    Callback mockCallback = mock(Callback.class);
    when(mockHealthChecker.getKafkaCallBack(any(), anyString(), anyString()))
        .thenReturn(mockCallback);

    // Setup producer to return a completed future
    RecordMetadata mockMetadata = new RecordMetadata(new TopicPartition("", 1),
        0L, 0, 0L, 0, 0);
    Future<RecordMetadata> mockFuture = CompletableFuture.completedFuture(mockMetadata);
    when(mockProducer.send(any(ProducerRecord.class), any(Callback.class))).thenReturn(mockFuture);

    eventProducer =
        new KafkaEventProducer(mockProducer, mockTopicConvention, mockHealthChecker, mockMetricUtils);
  }

  @DataProvider(name = "writabilityConfig")
  public Object[][] writabilityConfigProvider() {
    return new Object[][] {
        {true, "Writable"}, // canWrite = true, description
        {false, "ReadOnly"} // canWrite = false, description
    };
  }

  @Test(dataProvider = "writabilityConfig")
  public void testProduceMetadataChangeLogWithWritability(boolean canWrite, String description)
      throws Exception {
    eventProducer.setWritable(canWrite);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);

    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(urn);
    mcl.setEntityType(CORP_USER_ENTITY_NAME);
    mcl.setChangeType(ChangeType.UPSERT);

    Future<?> result = eventProducer.produceMetadataChangeLog(urn, mockAspectSpec, mcl);

    assertNotNull(result);

    if (canWrite) {
      // When writable, should send to Kafka
      verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
      // Result should be a real future from producer
      assertTrue(result instanceof Future);
    } else {
      // When not writable, should return completed future with empty optional
      verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
      assertTrue(result.isDone());
      Object value = result.get();
      assertTrue(value instanceof Optional);
      assertTrue(((Optional<?>) value).isEmpty());
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testProduceMetadataChangeLogTimeseriesWithWritability(
      boolean canWrite, String description) throws Exception {
    // Set writability
    eventProducer.setWritable(canWrite);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.isTimeseries()).thenReturn(true);

    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(urn);
    mcl.setEntityType(CORP_USER_ENTITY_NAME);
    mcl.setChangeType(ChangeType.UPSERT);

    Future<?> result = eventProducer.produceMetadataChangeLog(urn, mockAspectSpec, mcl);

    assertNotNull(result);

    if (canWrite) {
      verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
    } else {
      verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
      assertTrue(result.isDone());
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testProduceMetadataChangeProposalWithWritability(boolean canWrite, String description)
      throws Exception {
    // Set writability
    eventProducer.setWritable(canWrite);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);

    Future<?> result = eventProducer.produceMetadataChangeProposal(urn, mcp);

    assertNotNull(result);

    if (canWrite) {
      verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
    } else {
      verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
      assertTrue(result.isDone());
      Object value = result.get();
      assertTrue(value instanceof Optional);
      assertTrue(((Optional<?>) value).isEmpty());
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testProduceFailedMetadataChangeProposalAsyncWithWritability(
      boolean canWrite, String description) throws Exception {
    // Set writability
    eventProducer.setWritable(canWrite);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    OperationContext mockOpContext = mock(OperationContext.class);
    when(mockOpContext.traceException(any())).thenReturn("Error trace");

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);

    Set<Throwable> throwables = new HashSet<>();
    throwables.add(new RuntimeException("Test error"));

    Future<?> result = eventProducer.produceFailedMetadataChangeProposalAsync(opContext, mcp, throwables);

    assertNotNull(result);

    if (canWrite) {
      verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
    } else {
      verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
      assertTrue(result.isDone());
      Object value = result.get();
      assertTrue(value instanceof Optional);
      assertTrue(((Optional<?>) value).isEmpty());
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testProducePlatformEventWithWritability(boolean canWrite, String description)
      throws Exception {
    // Set writability
    eventProducer.setWritable(canWrite);

    String eventName = "testEvent_" + description;
    String key = "testKey_" + description;
    PlatformEvent event = new PlatformEvent();
    event.setName(eventName);
    PlatformEventHeader platformEventHeader = new PlatformEventHeader();
    platformEventHeader.setTimestampMillis(0L);
    event.setHeader(platformEventHeader);
    GenericPayload payload = new GenericPayload();
    payload.setValue(new ByteString(new ArrayList<>(), 0));
    payload.setContentType("application/json");
    event.setPayload(payload);

    Future<?> result = eventProducer.producePlatformEvent(eventName, key, event);

    assertNotNull(result);

    if (canWrite) {
      verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
    } else {
      verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
      assertTrue(result.isDone());
      Object value = result.get();
      assertTrue(value instanceof Optional);
      assertTrue(((Optional<?>) value).isEmpty());
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testProducePlatformEventWithNullKeyWritability(boolean canWrite, String description)
      throws Exception {
    // Set writability
    eventProducer.setWritable(canWrite);

    String eventName = "testEvent_" + description;
    PlatformEvent event = new PlatformEvent();
    event.setName(eventName);
    PlatformEventHeader platformEventHeader = new PlatformEventHeader();
    platformEventHeader.setTimestampMillis(0L);
    event.setHeader(platformEventHeader);
    GenericPayload payload = new GenericPayload();
    payload.setValue(new ByteString(new ArrayList<>(), 0));
    payload.setContentType("application/json");
    event.setPayload(payload);

    Future<?> result = eventProducer.producePlatformEvent(eventName, null, event);

    assertNotNull(result);

    if (canWrite) {
      verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
    } else {
      verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
      assertTrue(result.isDone());
    }
  }

  @Test
  public void testProduceDataHubUpgradeHistoryEventAlwaysWrites() {
    // Set to read-only
    eventProducer.setWritable(false);

    DataHubUpgradeHistoryEvent event = new DataHubUpgradeHistoryEvent();
    event.setVersion("1.0.0");

    // Should still write even when not writable
    eventProducer.produceDataHubUpgradeHistoryEvent(event);

    // Verify it was sent despite being read-only
    verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testProduceDataHubUpgradeHistoryEventWhenWritable() {
    // Set to writable
    eventProducer.setWritable(true);

    DataHubUpgradeHistoryEvent event = new DataHubUpgradeHistoryEvent();
    event.setVersion("2.0.0");

    eventProducer.produceDataHubUpgradeHistoryEvent(event);

    verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testSetWritableToggle() throws Exception {
    // Start writable
    eventProducer.setWritable(true);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setEntityUrn(urn);
    mcp1.setEntityType(CORP_USER_ENTITY_NAME);
    mcp1.setChangeType(ChangeType.UPSERT);


    eventProducer.produceMetadataChangeProposal(urn, mcp1);
    verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));

    // Set to read-only
    eventProducer.setWritable(false);

    MetadataChangeProposal mcp2 = new MetadataChangeProposal();
    mcp2.setEntityUrn(urn);
    mcp2.setEntityType(CORP_USER_ENTITY_NAME);
    mcp2.setChangeType(ChangeType.UPSERT);

    eventProducer.produceMetadataChangeProposal(urn, mcp2);
    // Still only 1 call from before
    verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));

    // Set back to writable
    eventProducer.setWritable(true);

    MetadataChangeProposal mcp3 = new MetadataChangeProposal();
    mcp3.setEntityUrn(urn);
    mcp3.setEntityType(CORP_USER_ENTITY_NAME);
    mcp3.setChangeType(ChangeType.UPSERT);

    eventProducer.produceMetadataChangeProposal(urn, mcp3);
    verify(mockProducer, times(2)).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testAllWriteOperationsBlockedWhenNotWritable() throws Exception {
    // Set to read-only
    eventProducer.setWritable(false);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    OperationContext mockOpContext = mock(OperationContext.class);
    when(mockOpContext.traceException(any())).thenReturn("Error trace");

    // 1. produceMetadataChangeLog
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(urn);
    Future<?> mclResult = eventProducer.produceMetadataChangeLog(urn, mockAspectSpec, mcl);
    assertTrue(mclResult.isDone());

    // 2. produceMetadataChangeProposal
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    Future<?> mcpResult = eventProducer.produceMetadataChangeProposal(urn, mcp);
    assertTrue(mcpResult.isDone());

    // 3. produceFailedMetadataChangeProposalAsync
    Set<Throwable> throwables = new HashSet<>();
    throwables.add(new RuntimeException("Test"));
    Future<?> fmcpResult =
        eventProducer.produceFailedMetadataChangeProposalAsync(mockOpContext, mcp, throwables);
    assertTrue(fmcpResult.isDone());

    // 4. producePlatformEvent
    PlatformEvent event = new PlatformEvent();
    event.setName("testEvent");
    Future<?> peResult = eventProducer.producePlatformEvent("testEvent", "key", event);
    assertTrue(peResult.isDone());

    // Verify no Kafka sends happened
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testWritabilityDuringMigration() throws Exception {
    // Simulate scenario where storage is being migrated
    eventProducer.setWritable(false);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    OperationContext mockOpContext = mock(OperationContext.class);
    when(mockOpContext.traceException(any())).thenReturn("Error trace");

    // All write operations should be blocked
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(urn);
    mcl.setEntityType(CORP_USER_ENTITY_NAME);
    mcl.setChangeType(ChangeType.UPSERT);
    eventProducer.produceMetadataChangeLog(urn, mockAspectSpec, mcl);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    eventProducer.produceMetadataChangeProposal(urn, mcp);

    Set<Throwable> throwables = new HashSet<>();
    throwables.add(new RuntimeException("Test"));
    eventProducer.produceFailedMetadataChangeProposalAsync(mockOpContext, mcp, throwables);

    PlatformEvent event = new PlatformEvent();
    event.setName("migrationEvent");
    PlatformEventHeader platformEventHeader = new PlatformEventHeader();
    platformEventHeader.setTimestampMillis(0L);
    event.setHeader(platformEventHeader);
    GenericPayload payload = new GenericPayload();
    payload.setValue(new ByteString(new ArrayList<>(), 0));
    payload.setContentType("application/json");
    event.setPayload(payload);
    eventProducer.producePlatformEvent("migrationEvent", "key", event);

    // No Kafka operations should have been executed
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));

    // After migration completes
    eventProducer.setWritable(true);

    // Writes should work again
    eventProducer.produceMetadataChangeProposal(urn, mcp);
    verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testMultipleOperationsInSequence() throws Exception {
    // Start writable
    eventProducer.setWritable(true);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);

    // Perform several operations
    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setEntityUrn(urn);
    mcp1.setEntityType(CORP_USER_ENTITY_NAME);
    mcp1.setChangeType(ChangeType.UPSERT);
    eventProducer.produceMetadataChangeProposal(urn, mcp1);

    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(urn);
    mcl.setEntityType(CORP_USER_ENTITY_NAME);
    mcl.setChangeType(ChangeType.UPSERT);
    eventProducer.produceMetadataChangeLog(urn, mockAspectSpec, mcl);

    PlatformEvent event = new PlatformEvent();
    event.setName("seqEvent");
    PlatformEventHeader platformEventHeader = new PlatformEventHeader();
    platformEventHeader.setTimestampMillis(0L);
    event.setHeader(platformEventHeader);
    GenericPayload payload = new GenericPayload();
    payload.setValue(new ByteString(new ArrayList<>(), 0));
    payload.setContentType("application/json");
    event.setPayload(payload);
    eventProducer.producePlatformEvent("seqEvent", "key", event);

    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));

    // Switch to read-only
    eventProducer.setWritable(false);

    // Try more operations
    MetadataChangeProposal mcp2 = new MetadataChangeProposal();
    mcp2.setEntityUrn(urn);
    mcp2.setEntityType(CORP_USER_ENTITY_NAME);
    mcp2.setChangeType(ChangeType.UPSERT);
    eventProducer.produceMetadataChangeProposal(urn, mcp2);

    eventProducer.produceMetadataChangeLog(urn, mockAspectSpec, mcl);

    // Count should not increase
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));

    // Switch back to writable
    eventProducer.setWritable(true);

    // Operations should work again
    MetadataChangeProposal mcp3 = new MetadataChangeProposal();
    mcp3.setEntityUrn(urn);
    mcp3.setEntityType(CORP_USER_ENTITY_NAME);
    mcp3.setChangeType(ChangeType.UPSERT);
    eventProducer.produceMetadataChangeProposal(urn, mcp3);
    verify(mockProducer, times(4)).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testFlushAlwaysWorks() {
    // Set to read-only
    eventProducer.setWritable(false);

    // Flush should still work
    eventProducer.flush();
    verify(mockProducer, times(1)).flush();

    // Set to writable
    eventProducer.setWritable(true);

    // Flush should still work
    eventProducer.flush();
    verify(mockProducer, times(2)).flush();
  }

  @Test
  public void testTopicNameGettersAlwaysWork() {
    // Set to read-only
    eventProducer.setWritable(false);

    // Topic name getters should still work
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);

    String mclTopic = eventProducer.getMetadataChangeLogTopicName(mockAspectSpec);
    assertNotNull(mclTopic);
    verify(mockTopicConvention, times(1)).getMetadataChangeLogVersionedTopicName();

    String mcpTopic = eventProducer.getMetadataChangeProposalTopicName();
    assertNotNull(mcpTopic);
    verify(mockTopicConvention, times(1)).getMetadataChangeProposalTopicName();

    String peTopic = eventProducer.getPlatformEventTopicName();
    assertNotNull(peTopic);
    verify(mockTopicConvention, times(1)).getPlatformEventTopicName();
  }

  @Test
  public void testTimeseriesTopicSelection() {
    eventProducer.setWritable(true);

    AspectSpec timeseriesAspect = mock(AspectSpec.class);
    when(timeseriesAspect.isTimeseries()).thenReturn(true);

    String topic = eventProducer.getMetadataChangeLogTopicName(timeseriesAspect);
    assertNotNull(topic);
    verify(mockTopicConvention, times(1)).getMetadataChangeLogTimeseriesTopicName();

    AspectSpec normalAspect = mock(AspectSpec.class);
    when(normalAspect.isTimeseries()).thenReturn(false);

    String normalTopic = eventProducer.getMetadataChangeLogTopicName(normalAspect);
    assertNotNull(normalTopic);
    verify(mockTopicConvention, times(2)).getMetadataChangeLogVersionedTopicName();
  }

  @Test
  public void testUpgradeHistoryEventBypassesWritabilityCheck() {
    // Explicitly test that upgrade history events bypass the writability check
    eventProducer.setWritable(false);

    DataHubUpgradeHistoryEvent event1 = new DataHubUpgradeHistoryEvent();
    event1.setVersion("1.0.0");
    eventProducer.produceDataHubUpgradeHistoryEvent(event1);

    DataHubUpgradeHistoryEvent event2 = new DataHubUpgradeHistoryEvent();
    event2.setVersion("1.1.0");
    eventProducer.produceDataHubUpgradeHistoryEvent(event2);

    // Both should have been sent despite being read-only
    verify(mockProducer, times(2)).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  public void testProducerRecordContainsCorrectTopicWhenWritable() throws Exception {
    eventProducer.setWritable(true);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);

    // Test MCL topic
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(urn);
    mcl.setEntityType(CORP_USER_ENTITY_NAME);
    mcl.setChangeType(ChangeType.UPSERT);
    eventProducer.produceMetadataChangeLog(urn, mockAspectSpec, mcl);

    // Verify the correct topic was used (through topic convention call)
    verify(mockTopicConvention, times(1)).getMetadataChangeLogVersionedTopicName();

    // Test MCP topic
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    eventProducer.produceMetadataChangeProposal(urn, mcp);

    verify(mockTopicConvention, times(1)).getMetadataChangeProposalTopicName();
  }

  @Test
  public void testCallbackInvokedWhenWritable() throws Exception {
    eventProducer.setWritable(true);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);

    eventProducer.produceMetadataChangeProposal(urn, mcp);

    // Verify health checker callback was requested
    verify(mockHealthChecker, times(1))
        .getKafkaCallBack(eq(mockMetricUtils), eq("MCP"), eq(urn.toString()));
  }

  @Test
  public void testCallbackNotInvokedWhenNotWritable() throws Exception {
    eventProducer.setWritable(false);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);

    eventProducer.produceMetadataChangeProposal(urn, mcp);

    // Verify health checker callback was never requested
    verify(mockHealthChecker, never()).getKafkaCallBack(any(), anyString(), anyString());
  }

  @Test
  public void testReturnedFuturesAreCompleted() throws ExecutionException, InterruptedException {
    eventProducer.setWritable(false);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);

    // Test all methods return completed futures when not writable
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(urn);
    mcl.setEntityType(CORP_USER_ENTITY_NAME);
    mcl.setChangeType(ChangeType.UPSERT);
    Future<?> mclFuture = eventProducer.produceMetadataChangeLog(urn, mockAspectSpec, mcl);
    assertTrue(mclFuture.isDone());
    assertTrue(mclFuture.get() instanceof Optional);
    assertTrue(((Optional<?>) mclFuture.get()).isEmpty());

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    Future<?> mcpFuture = eventProducer.produceMetadataChangeProposal(urn, mcp);
    assertTrue(mcpFuture.isDone());
    assertTrue(mcpFuture.get() instanceof Optional);
    assertTrue(((Optional<?>) mcpFuture.get()).isEmpty());

    OperationContext mockOpContext = mock(OperationContext.class);
    when(mockOpContext.traceException(any())).thenReturn("Error");
    Set<Throwable> throwables = new HashSet<>();
    throwables.add(new RuntimeException("Test"));
    Future<?> fmcpFuture =
        eventProducer.produceFailedMetadataChangeProposalAsync(mockOpContext, mcp, throwables);
    assertTrue(fmcpFuture.isDone());
    assertTrue(fmcpFuture.get() instanceof Optional);
    assertTrue(((Optional<?>) fmcpFuture.get()).isEmpty());

    PlatformEvent event = new PlatformEvent();
    event.setName("test");
    Future<?> peFuture = eventProducer.producePlatformEvent("test", "key", event);
    assertTrue(peFuture.isDone());
    assertTrue(peFuture.get() instanceof Optional);
    assertTrue(((Optional<?>) peFuture.get()).isEmpty());
  }

  @Test
  public void testWritabilityStateIndependentAcrossInstances() throws Exception {
    // Create a second instance
    KafkaEventProducer secondProducer =
        new KafkaEventProducer(
            mockProducer, mockTopicConvention, mockHealthChecker, mockMetricUtils);

    eventProducer.setWritable(false);

    secondProducer.setWritable(true);

    Urn urn = UrnUtils.getUrn(TEST_URN_STRING);
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setChangeType(ChangeType.UPSERT);

    // First instance operations blocked
    eventProducer.produceMetadataChangeProposal(urn, mcp);
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));

    // Second instance operations work
    secondProducer.produceMetadataChangeProposal(urn, mcp);
    verify(mockProducer, times(1)).send(any(ProducerRecord.class), any(Callback.class));
  }
}
