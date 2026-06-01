package com.linkedin.metadata.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueMessageHeader;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.mxe.GenericPayload;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavioral tests for {@link PgQueueEventProducer}. We verify the producer enqueues a
 * Confluent-formatted payload to the queue store on the DUHE topic, and skips quietly when the
 * schema id cannot be resolved.
 */
public class PgQueueEventProducerTest {

  private static final String DUHE_TOPIC = "DataHubUpgradeHistory_v1";
  private static final String MCL_VERSIONED_TOPIC = "MetadataChangeLog_Versioned_v1";
  private static final String MCL_TIMESERIES_TOPIC = "MetadataChangeLog_Timeseries_v1";
  private static final String MCP_TOPIC = "MetadataChangeProposal_v1";
  private static final String FMCP_TOPIC = "FailedMetadataChangeProposal_v1";
  private static final String PE_TOPIC = "PlatformEvent_v1";
  private static final int DUHE_SCHEMA_ID = 100;
  private static final int MCL_SCHEMA_ID = 200;
  private static final int MCP_SCHEMA_ID = 300;
  private static final int FMCP_SCHEMA_ID = 400;
  private static final int PE_SCHEMA_ID = 500;
  private static final QueueTopicDefaults DEFAULTS =
      new QueueTopicDefaults(1, 0, 0L, 0L, false, null);

  private MetadataQueueStore queueStore;
  private TopicConvention topicConvention;
  private SchemaRegistryService schemaRegistryService;
  private PgQueueEventProducer producer;

  @BeforeMethod
  public void setUp() {
    queueStore = mock(MetadataQueueStore.class);
    topicConvention = mock(TopicConvention.class);
    schemaRegistryService = mock(SchemaRegistryService.class);
    when(topicConvention.getDataHubUpgradeHistoryTopicName()).thenReturn(DUHE_TOPIC);
    producer =
        new PgQueueEventProducer(
            queueStore,
            topicConvention,
            schemaRegistryService,
            DEFAULTS,
            PgQueuePayloadCompression.NONE);
  }

  @Test
  public void testProduceDataHubUpgradeHistoryEventEnqueuesConfluentFormattedBytes() {
    when(schemaRegistryService.getSchemaIdForTopic(DUHE_TOPIC))
        .thenReturn(Optional.of(DUHE_SCHEMA_ID));
    when(queueStore.enqueue(
            eq(DUHE_TOPIC),
            any(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE)))
        .thenReturn(mock(QueueMessageHandle.class));

    DataHubUpgradeHistoryEvent event = new DataHubUpgradeHistoryEvent().setVersion("0.10.0-test");
    producer.produceDataHubUpgradeHistoryEvent(event);

    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<String> routingKeyCaptor = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Optional<String>> contentTypeCaptor =
        (ArgumentCaptor<Optional<String>>) (Object) ArgumentCaptor.forClass(Optional.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<QueueMessageHeader>> headersCaptor =
        (ArgumentCaptor<List<QueueMessageHeader>>) (Object) ArgumentCaptor.forClass(List.class);

    verify(queueStore, times(1))
        .enqueue(
            eq(DUHE_TOPIC),
            routingKeyCaptor.capture(),
            eq(DEFAULTS),
            eq(QueueTopicMetadata.DEFAULT_PRIORITY),
            payloadCaptor.capture(),
            contentTypeCaptor.capture(),
            headersCaptor.capture(),
            eq(PgQueuePayloadCompression.NONE));

    assertEquals(routingKeyCaptor.getValue(), "0.10.0-test");
    assertEquals(
        contentTypeCaptor.getValue(),
        Optional.of(PgQueueEventProducer.CONFLUENT_AVRO_CONTENT_TYPE));
    assertEquals(headersCaptor.getValue(), List.of());

    byte[] payload = payloadCaptor.getValue();
    assertNotNull(payload);
    assertEquals(payload[0], PgQueueEventProducer.CONFLUENT_MAGIC_BYTE, "Magic byte must be 0x00");
    int schemaId = ByteBuffer.wrap(payload, 1, 4).getInt();
    assertEquals(schemaId, DUHE_SCHEMA_ID, "4-byte schema id prefix must match registry id");
  }

  @Test
  public void testProduceDataHubUpgradeHistoryEventSkipsWhenSchemaIdUnknown() {
    when(schemaRegistryService.getSchemaIdForTopic(DUHE_TOPIC)).thenReturn(Optional.empty());

    producer.produceDataHubUpgradeHistoryEvent(new DataHubUpgradeHistoryEvent().setVersion("v"));

    verify(queueStore, never())
        .enqueue(any(), any(), any(), anyInt(), any(byte[].class), any(), any(), any());
  }

  @Test
  public void testProduceDataHubUpgradeHistoryEventDoesNotThrowWhenStoreFails() {
    when(schemaRegistryService.getSchemaIdForTopic(DUHE_TOPIC))
        .thenReturn(Optional.of(DUHE_SCHEMA_ID));
    when(queueStore.enqueue(any(), any(), any(), anyInt(), any(byte[].class), any(), any(), any()))
        .thenThrow(new IllegalStateException("simulated DB failure"));

    // Behavior under test: the upgrade success must NOT be masked by a queue write failure.
    producer.produceDataHubUpgradeHistoryEvent(new DataHubUpgradeHistoryEvent().setVersion("v"));
  }

  @Test
  public void testProduceMetadataChangeLogEnqueuesWhenSchemaIdKnown() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.isTimeseries()).thenReturn(false);
    when(topicConvention.getMetadataChangeLogVersionedTopicName()).thenReturn(MCL_VERSIONED_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(MCL_VERSIONED_TOPIC))
        .thenReturn(Optional.of(MCL_SCHEMA_ID));
    when(queueStore.enqueue(
            eq(MCL_VERSIONED_TOPIC),
            any(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE)))
        .thenReturn(mock(QueueMessageHandle.class));

    MetadataChangeLog mcl =
        new MetadataChangeLog()
            .setAspectName("datasetProperties")
            .setEntityType("dataset")
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(urn);

    producer.produceMetadataChangeLog(urn, aspectSpec, mcl);

    verify(queueStore, times(1))
        .enqueue(
            eq(MCL_VERSIONED_TOPIC),
            eq(urn.toString()),
            any(),
            anyInt(),
            any(),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE));
  }

  @Test
  public void testProduceMetadataChangeLogSkipsWhenSchemaIdUnknown() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.isTimeseries()).thenReturn(false);
    when(topicConvention.getMetadataChangeLogVersionedTopicName()).thenReturn(MCL_VERSIONED_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(MCL_VERSIONED_TOPIC))
        .thenReturn(Optional.empty());

    MetadataChangeLog mcl =
        new MetadataChangeLog()
            .setAspectName("datasetProperties")
            .setEntityType("dataset")
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(urn);

    producer.produceMetadataChangeLog(urn, aspectSpec, mcl);

    verify(queueStore, never())
        .enqueue(any(), any(), any(), anyInt(), any(byte[].class), any(), any(), any());
  }

  @Test
  public void testProduceMetadataChangeProposalEnqueuesToMcpTopic() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    when(topicConvention.getMetadataChangeProposalTopicName()).thenReturn(MCP_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(MCP_TOPIC))
        .thenReturn(Optional.of(MCP_SCHEMA_ID));
    when(queueStore.enqueue(
            eq(MCP_TOPIC),
            any(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE)))
        .thenReturn(mock(QueueMessageHandle.class));

    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType("dataset")
            .setAspectName("datasetProperties")
            .setChangeType(ChangeType.UPSERT);

    Future<?> result = producer.produceMetadataChangeProposal(urn, mcp);

    assertNotNull(result);
    assertEquals(result.isDone(), true);

    verify(queueStore, times(1))
        .enqueue(
            eq(MCP_TOPIC),
            eq(urn.toString()),
            any(),
            anyInt(),
            any(),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE));
  }

  @Test
  public void testProduceFailedMetadataChangeProposalAsyncEnqueuesToFmcpTopic() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    when(topicConvention.getFailedMetadataChangeProposalTopicName()).thenReturn(FMCP_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(FMCP_TOPIC))
        .thenReturn(Optional.of(FMCP_SCHEMA_ID));
    when(queueStore.enqueue(
            eq(FMCP_TOPIC),
            any(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE)))
        .thenReturn(mock(QueueMessageHandle.class));

    OperationContext opContext = mock(OperationContext.class);
    when(opContext.traceException(any())).thenReturn("traced error");

    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType("dataset")
            .setAspectName("datasetProperties")
            .setChangeType(ChangeType.UPSERT);

    Set<Throwable> throwables = Set.of(new RuntimeException("simulated failure"));

    Future<?> result =
        producer.produceFailedMetadataChangeProposalAsync(opContext, mcp, throwables);

    assertNotNull(result);
    assertEquals(result.isDone(), true);

    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(queueStore, times(1))
        .enqueue(
            eq(FMCP_TOPIC),
            eq(urn.toString()),
            any(),
            anyInt(),
            payloadCaptor.capture(),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE));

    byte[] payload = payloadCaptor.getValue();
    assertNotNull(payload);
    assertEquals(payload[0], PgQueueEventProducer.CONFLUENT_MAGIC_BYTE);
    int schemaId = ByteBuffer.wrap(payload, 1, 4).getInt();
    assertEquals(schemaId, FMCP_SCHEMA_ID);
  }

  @Test
  public void testProducePlatformEventEnqueuesWithKeyAsRoutingKey() throws Exception {
    when(topicConvention.getPlatformEventTopicName()).thenReturn(PE_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(PE_TOPIC)).thenReturn(Optional.of(PE_SCHEMA_ID));
    when(queueStore.enqueue(
            eq(PE_TOPIC),
            any(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE)))
        .thenReturn(mock(QueueMessageHandle.class));

    PlatformEvent event = new PlatformEvent();
    event.setName("EntityChangeEvent_v1");
    event.setHeader(new PlatformEventHeader().setTimestampMillis(0L));
    event.setPayload(
        new GenericPayload()
            .setValue(com.linkedin.data.ByteString.copy("{}".getBytes()))
            .setContentType("application/json"));

    Future<?> result = producer.producePlatformEvent("EntityChangeEvent_v1", "myKey", event);

    assertNotNull(result);
    assertEquals(result.isDone(), true);

    verify(queueStore, times(1))
        .enqueue(
            eq(PE_TOPIC),
            eq("myKey"),
            any(),
            anyInt(),
            any(),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE));
  }

  @Test
  public void testProducePlatformEventUsesNameWhenKeyIsNull() throws Exception {
    when(topicConvention.getPlatformEventTopicName()).thenReturn(PE_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(PE_TOPIC)).thenReturn(Optional.of(PE_SCHEMA_ID));
    when(queueStore.enqueue(
            eq(PE_TOPIC),
            any(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE)))
        .thenReturn(mock(QueueMessageHandle.class));

    PlatformEvent event = new PlatformEvent();
    event.setName("EntityChangeEvent_v1");
    event.setHeader(new PlatformEventHeader().setTimestampMillis(0L));
    event.setPayload(
        new GenericPayload()
            .setValue(com.linkedin.data.ByteString.copy("{}".getBytes()))
            .setContentType("application/json"));

    Future<?> result = producer.producePlatformEvent("EntityChangeEvent_v1", null, event);

    assertNotNull(result);
    assertEquals(result.isDone(), true);

    verify(queueStore, times(1))
        .enqueue(
            eq(PE_TOPIC),
            eq("EntityChangeEvent_v1"),
            any(),
            anyInt(),
            any(),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE));
  }

  @Test
  public void testFlushIsNoOp() {
    producer.flush();
  }

  @Test
  public void testGetMetadataChangeLogTopicNameTimeseries() {
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.isTimeseries()).thenReturn(true);
    when(topicConvention.getMetadataChangeLogTimeseriesTopicName())
        .thenReturn(MCL_TIMESERIES_TOPIC);

    assertEquals(producer.getMetadataChangeLogTopicName(aspectSpec), MCL_TIMESERIES_TOPIC);
  }

  @Test
  public void testPublishRawTopicConfluentAvroEnqueuesWhenSchemaPresent() throws Exception {
    when(schemaRegistryService.getSchemaIdForTopic(MCP_TOPIC))
        .thenReturn(Optional.of(MCP_SCHEMA_ID));
    when(queueStore.enqueue(
            eq(MCP_TOPIC),
            any(),
            any(QueueTopicDefaults.class),
            anyInt(),
            any(byte[].class),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE)))
        .thenReturn(mock(QueueMessageHandle.class));

    List<Schema.Field> fields =
        List.of(new Schema.Field("payload", Schema.create(Schema.Type.STRING), null, (Object) ""));
    Schema schema = Schema.createRecord("RawRecord", null, null, false, fields);
    GenericRecord record = new GenericData.Record(schema);
    record.put("payload", "x");

    Future<?> result =
        producer.publishRawTopicConfluentAvro(MCP_TOPIC, "routing-key", record, "raw");

    assertNotNull(result);
    assertTrue(result.isDone());
    verify(queueStore, times(1))
        .enqueue(
            eq(MCP_TOPIC),
            eq("routing-key"),
            any(),
            anyInt(),
            any(),
            any(),
            any(),
            eq(PgQueuePayloadCompression.NONE));
  }

  @Test
  public void testProduceMetadataChangeProposalSkipsWhenSchemaIdUnknown() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    when(topicConvention.getMetadataChangeProposalTopicName()).thenReturn(MCP_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(MCP_TOPIC)).thenReturn(Optional.empty());

    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType("dataset")
            .setAspectName("datasetProperties")
            .setChangeType(ChangeType.UPSERT);

    producer.produceMetadataChangeProposal(urn, mcp);

    verify(queueStore, never())
        .enqueue(any(), any(), any(), anyInt(), any(byte[].class), any(), any(), any());
  }

  @Test
  public void testProduceFailedMetadataChangeProposalSkipsWhenSchemaIdUnknown() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    when(topicConvention.getFailedMetadataChangeProposalTopicName()).thenReturn(FMCP_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(FMCP_TOPIC)).thenReturn(Optional.empty());

    OperationContext opContext = mock(OperationContext.class);
    when(opContext.traceException(any())).thenReturn("traced error");
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType("dataset")
            .setAspectName("datasetProperties")
            .setChangeType(ChangeType.UPSERT);

    Future<?> result =
        producer.produceFailedMetadataChangeProposalAsync(
            opContext, mcp, Set.of(new RuntimeException("err")));

    assertTrue(result.isDone());
    verify(queueStore, never())
        .enqueue(any(), any(), any(), anyInt(), any(byte[].class), any(), any(), any());
  }

  @Test
  public void testProducePlatformEventSkipsWhenSchemaIdUnknown() throws Exception {
    when(topicConvention.getPlatformEventTopicName()).thenReturn(PE_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(PE_TOPIC)).thenReturn(Optional.empty());

    PlatformEvent event = new PlatformEvent();
    event.setName("EntityChangeEvent_v1");
    event.setHeader(new PlatformEventHeader().setTimestampMillis(0L));
    event.setPayload(
        new GenericPayload()
            .setValue(com.linkedin.data.ByteString.copy("{}".getBytes()))
            .setContentType("application/json"));

    Future<?> result = producer.producePlatformEvent("EntityChangeEvent_v1", "key", event);

    assertTrue(result.isDone());
    verify(queueStore, never())
        .enqueue(any(), any(), any(), anyInt(), any(byte[].class), any(), any(), any());
  }

  @Test
  public void testProduceMetadataChangeLogReturnsFailedFutureOnConversionError() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.isTimeseries()).thenReturn(false);
    when(topicConvention.getMetadataChangeLogVersionedTopicName()).thenReturn(MCL_VERSIONED_TOPIC);

    MetadataChangeLog mcl =
        new MetadataChangeLog()
            .setAspectName("datasetProperties")
            .setEntityType("dataset")
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(urn);

    try (MockedStatic<EventUtils> eventUtils = org.mockito.Mockito.mockStatic(EventUtils.class)) {
      eventUtils
          .when(() -> EventUtils.pegasusToAvroMCL(mcl))
          .thenThrow(new IOException("conversion failed"));

      Future<?> result = producer.produceMetadataChangeLog(urn, aspectSpec, mcl);

      assertTrue(result.isDone());
      try {
        result.get();
        org.testng.Assert.fail("Expected ExecutionException");
      } catch (ExecutionException e) {
        org.testng.Assert.assertTrue(e.getCause() instanceof ModelConversionException);
      }
      verify(queueStore, never())
          .enqueue(any(), any(), any(), anyInt(), any(byte[].class), any(), any(), any());
    }
  }

  @Test
  public void testProduceMetadataChangeProposalCompletesWhenEnqueueThrows() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    when(topicConvention.getMetadataChangeProposalTopicName()).thenReturn(MCP_TOPIC);
    when(schemaRegistryService.getSchemaIdForTopic(MCP_TOPIC))
        .thenReturn(Optional.of(MCP_SCHEMA_ID));
    when(queueStore.enqueue(any(), any(), any(), anyInt(), any(byte[].class), any(), any(), any()))
        .thenThrow(new IllegalStateException("db down"));

    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(urn)
            .setEntityType("dataset")
            .setAspectName("datasetProperties")
            .setChangeType(ChangeType.UPSERT);

    Future<?> result = producer.produceMetadataChangeProposal(urn, mcp);

    assertTrue(result.isDone());
    org.testng.Assert.assertNull(result.get());
  }
}
