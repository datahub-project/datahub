package com.linkedin.metadata.trace;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueuePayloadCodec;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueLogPeekRow;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueueMcpFailedTracePortTest {

  private static final String TOPIC = "FailedMetadataChangeProposal_v1";
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent,PROD)");
  private static final String TRACE_ID = "trace-abc-123";
  private static final String ASPECT_NAME = "status";

  private ExecutorService executor;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    executor = MoreExecutors.newDirectExecutorService();
  }

  @AfterMethod
  public void tearDown() {
    executor.shutdown();
  }

  @Test
  public void findMessages_returnsEmptyMap_whenTopicMissing() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC)).thenReturn(Optional.empty());

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);

    PgQueueMcpFailedTracePort port =
        new PgQueueMcpFailedTracePort(store, TOPIC, executor, 30L, 100, 3, deserializer);

    Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>> result =
        port.findMessages(
            Collections.singletonMap(URN, List.of(ASPECT_NAME)), TRACE_ID, 1_700_000_000_000L);

    assertTrue(result.containsKey(URN));
    assertTrue(result.get(URN).isEmpty());
  }

  @Test
  public void findMessages_returnsMatchedMessages_whenTraceIdAndAspectMatch() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC))
        .thenReturn(Optional.of(new QueueTopicMetadata(1L, 8, Optional.empty())));
    when(store.minEnqueueSeqAtOrAfter(anyLong(), anyInt(), any())).thenReturn(OptionalLong.empty());
    when(store.minEnqueueSeq(anyLong(), anyInt())).thenReturn(OptionalLong.of(1L));

    int partition = PgQueueTracePartitionUtil.partitionForUrn(URN, 8);

    SystemMetadata sysMeta = new SystemMetadata();
    StringMap props = new StringMap();
    props.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, TRACE_ID);
    sysMeta.setProperties(props);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setAspectName(ASPECT_NAME);
    mcp.setSystemMetadata(sysMeta);

    FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
    fmcp.setMetadataChangeProposal(mcp);

    byte[] payload = new byte[] {1, 2, 3};
    QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.now(), 1L, partition, 5L);
    QueueLogPeekRow row =
        new QueueLogPeekRow(
            handle,
            5,
            payload,
            Optional.empty(),
            PgQueuePayloadCompression.NONE,
            Collections.emptyList(),
            URN.toString());

    when(store.peekTopicLog(eq(1L), any(), eq(100))).thenReturn(List.of(row));

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    GenericRecord avroRecord = mock(GenericRecord.class);
    when(deserializer.deserialize(eq(TOPIC), any(byte[].class))).thenReturn(avroRecord);

    try (MockedStatic<PgQueuePayloadCodec> codecMock = mockStatic(PgQueuePayloadCodec.class);
        MockedStatic<EventUtils> eventMock = mockStatic(EventUtils.class)) {

      codecMock
          .when(() -> PgQueuePayloadCodec.decode(payload, PgQueuePayloadCompression.NONE))
          .thenReturn(payload);
      eventMock.when(() -> EventUtils.avroToPegasusFailedMCP(avroRecord)).thenReturn(fmcp);

      PgQueueMcpFailedTracePort port =
          new PgQueueMcpFailedTracePort(store, TOPIC, executor, 30L, 100, 3, deserializer);

      Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>> result =
          port.findMessages(
              Collections.singletonMap(URN, List.of(ASPECT_NAME)), TRACE_ID, 1_700_000_000_000L);

      assertTrue(result.containsKey(URN));
      assertTrue(result.get(URN).containsKey(ASPECT_NAME));
      assertEquals(result.get(URN).get(ASPECT_NAME).getSecond(), sysMeta);
    }
  }

  @Test
  public void findMessages_returnsEmptyInnerMap_whenNoMessagesMatch() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);
    when(store.fetchTopic(TOPIC))
        .thenReturn(Optional.of(new QueueTopicMetadata(1L, 8, Optional.empty())));
    when(store.minEnqueueSeqAtOrAfter(anyLong(), anyInt(), any())).thenReturn(OptionalLong.empty());
    when(store.minEnqueueSeq(anyLong(), anyInt())).thenReturn(OptionalLong.of(1L));
    when(store.peekTopicLog(anyLong(), any(), anyInt())).thenReturn(Collections.emptyList());

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);

    PgQueueMcpFailedTracePort port =
        new PgQueueMcpFailedTracePort(store, TOPIC, executor, 30L, 100, 3, deserializer);

    Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>> result =
        port.findMessages(
            Collections.singletonMap(URN, List.of(ASPECT_NAME)), TRACE_ID, 1_700_000_000_000L);

    assertTrue(result.containsKey(URN));
    assertTrue(result.get(URN).isEmpty());
  }

  @Test
  public void read_returnsEmpty_whenNull() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);

    PgQueueMcpFailedTracePort port =
        new PgQueueMcpFailedTracePort(store, TOPIC, executor, 30L, 100, 3, deserializer);

    Optional<FailedMetadataChangeProposal> result = port.read(null);
    assertFalse(result.isPresent());
  }

  @Test
  public void read_returnsPresent_whenValidRecord() {
    MetadataQueueStore store = mock(MetadataQueueStore.class);

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
    GenericRecord avroRecord = mock(GenericRecord.class);
    FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();

    try (MockedStatic<EventUtils> eventMock = mockStatic(EventUtils.class)) {
      eventMock.when(() -> EventUtils.avroToPegasusFailedMCP(avroRecord)).thenReturn(fmcp);

      PgQueueMcpFailedTracePort port =
          new PgQueueMcpFailedTracePort(store, TOPIC, executor, 30L, 100, 3, deserializer);

      Optional<FailedMetadataChangeProposal> result = port.read(avroRecord);
      assertTrue(result.isPresent());
      assertEquals(result.get(), fmcp);
    }
  }

  @Test
  public void matchFailed_returnsMatch_whenTraceIdAndAspectAlign() {
    SystemMetadata sysMeta = new SystemMetadata();
    StringMap props = new StringMap();
    props.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, TRACE_ID);
    sysMeta.setProperties(props);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setAspectName(ASPECT_NAME);
    mcp.setSystemMetadata(sysMeta);

    FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
    fmcp.setMetadataChangeProposal(mcp);

    GenericRecord avroRecord = mock(GenericRecord.class);

    try (MockedStatic<EventUtils> eventMock = mockStatic(EventUtils.class)) {
      eventMock.when(() -> EventUtils.avroToPegasusFailedMCP(avroRecord)).thenReturn(fmcp);

      ConsumerRecord<String, GenericRecord> cr =
          new ConsumerRecord<>(TOPIC, 0, 1L, URN.toString(), avroRecord);

      MetadataQueueStore store = mock(MetadataQueueStore.class);
      when(store.fetchTopic(TOPIC))
          .thenReturn(Optional.of(new QueueTopicMetadata(1L, 8, Optional.empty())));
      when(store.minEnqueueSeqAtOrAfter(anyLong(), anyInt(), any()))
          .thenReturn(OptionalLong.empty());
      when(store.minEnqueueSeq(anyLong(), anyInt())).thenReturn(OptionalLong.of(1L));

      int partition = PgQueueTracePartitionUtil.partitionForUrn(URN, 8);
      byte[] payload = new byte[] {1, 2, 3};
      QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.now(), 1L, partition, 1L);
      QueueLogPeekRow row =
          new QueueLogPeekRow(
              handle,
              5,
              payload,
              Optional.empty(),
              PgQueuePayloadCompression.NONE,
              Collections.emptyList(),
              URN.toString());
      when(store.peekTopicLog(eq(1L), any(), eq(100))).thenReturn(List.of(row));

      @SuppressWarnings("unchecked")
      Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
      when(deserializer.deserialize(eq(TOPIC), any(byte[].class))).thenReturn(avroRecord);

      try (MockedStatic<PgQueuePayloadCodec> codecMock = mockStatic(PgQueuePayloadCodec.class)) {
        codecMock
            .when(() -> PgQueuePayloadCodec.decode(payload, PgQueuePayloadCompression.NONE))
            .thenReturn(payload);

        PgQueueMcpFailedTracePort port =
            new PgQueueMcpFailedTracePort(store, TOPIC, executor, 30L, 100, 3, deserializer);

        Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>> result =
            port.findMessages(
                Collections.singletonMap(URN, List.of(ASPECT_NAME)), TRACE_ID, 1_700_000_000_000L);

        assertTrue(result.get(URN).containsKey(ASPECT_NAME));
        assertEquals(result.get(URN).get(ASPECT_NAME).getSecond(), sysMeta);
      }
    }
  }

  @Test
  public void matchFailed_returnsFalse_whenTraceIdDoesNotMatch() {
    SystemMetadata sysMeta = new SystemMetadata();
    StringMap props = new StringMap();
    props.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, "other-trace-id");
    sysMeta.setProperties(props);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setAspectName(ASPECT_NAME);
    mcp.setSystemMetadata(sysMeta);

    FailedMetadataChangeProposal fmcp = new FailedMetadataChangeProposal();
    fmcp.setMetadataChangeProposal(mcp);

    GenericRecord avroRecord = mock(GenericRecord.class);

    try (MockedStatic<EventUtils> eventMock = mockStatic(EventUtils.class);
        MockedStatic<PgQueuePayloadCodec> codecMock = mockStatic(PgQueuePayloadCodec.class)) {
      eventMock.when(() -> EventUtils.avroToPegasusFailedMCP(avroRecord)).thenReturn(fmcp);

      MetadataQueueStore store = mock(MetadataQueueStore.class);
      when(store.fetchTopic(TOPIC))
          .thenReturn(Optional.of(new QueueTopicMetadata(1L, 8, Optional.empty())));
      when(store.minEnqueueSeqAtOrAfter(anyLong(), anyInt(), any()))
          .thenReturn(OptionalLong.empty());
      when(store.minEnqueueSeq(anyLong(), anyInt())).thenReturn(OptionalLong.of(1L));

      int partition = PgQueueTracePartitionUtil.partitionForUrn(URN, 8);
      byte[] payload = new byte[] {1, 2, 3};
      QueueMessageHandle handle = new QueueMessageHandle(1L, Instant.now(), 1L, partition, 1L);
      QueueLogPeekRow row =
          new QueueLogPeekRow(
              handle,
              5,
              payload,
              Optional.empty(),
              PgQueuePayloadCompression.NONE,
              Collections.emptyList(),
              URN.toString());
      when(store.peekTopicLog(eq(1L), any(), eq(100))).thenReturn(List.of(row));

      @SuppressWarnings("unchecked")
      Deserializer<GenericRecord> deserializer = mock(Deserializer.class);
      when(deserializer.deserialize(eq(TOPIC), any(byte[].class))).thenReturn(avroRecord);

      codecMock
          .when(() -> PgQueuePayloadCodec.decode(payload, PgQueuePayloadCompression.NONE))
          .thenReturn(payload);

      PgQueueMcpFailedTracePort port =
          new PgQueueMcpFailedTracePort(store, TOPIC, executor, 30L, 100, 3, deserializer);

      Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>> result =
          port.findMessages(
              Collections.singletonMap(URN, List.of(ASPECT_NAME)), TRACE_ID, 1_700_000_000_000L);

      assertTrue(result.containsKey(URN));
      assertTrue(result.get(URN).isEmpty());
    }
  }
}
