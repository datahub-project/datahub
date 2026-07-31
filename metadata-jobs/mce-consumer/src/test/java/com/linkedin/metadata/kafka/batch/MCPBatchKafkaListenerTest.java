package com.linkedin.metadata.kafka.batch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.kafka.context.inbound.DefaultInboundBatchAffinityResolver;
import com.linkedin.metadata.kafka.context.inbound.InboundBatchAffinityResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MCPBatchKafkaListenerTest {

  private static final String CONSUMER_GROUP = "generic-mce-consumer-job-client";

  private BatchMetadataChangeProposalsProcessor processor;
  private InboundBatchAffinityResolver batchAffinityResolver;
  private OperationContext systemOperationContext;
  private MCPBatchKafkaListener listener;

  @BeforeMethod
  public void setUp() {
    processor = mock(BatchMetadataChangeProposalsProcessor.class);
    batchAffinityResolver = mock(InboundBatchAffinityResolver.class);
    systemOperationContext = mock(OperationContext.class);
    listener =
        new MCPBatchKafkaListener(
            processor, batchAffinityResolver, systemOperationContext, CONSUMER_GROUP);
  }

  @Test
  public void testOnMessageEmptyBatchIsNoOp() {
    listener.onMessage(Collections.emptyList());

    verify(processor, never()).consume(any(OperationContext.class), anyList());
    verify(batchAffinityResolver, never()).partition(any(), any(), any());
  }

  @Test
  public void testOnMessageDispatchesEachAffinitySlice() {
    ConsumerRecord<String, GenericRecord> record1 = mock(ConsumerRecord.class);
    ConsumerRecord<String, GenericRecord> record2 = mock(ConsumerRecord.class);
    List<ConsumerRecord<String, GenericRecord>> records = List.of(record1, record2);

    OperationContext sliceContextA = mock(OperationContext.class);
    OperationContext sliceContextB = mock(OperationContext.class);

    when(batchAffinityResolver.partition(records, CONSUMER_GROUP, systemOperationContext))
        .thenReturn(
            List.of(
                new InboundBatchAffinityResolver.Slice<>(sliceContextA, List.of(record1)),
                new InboundBatchAffinityResolver.Slice<>(sliceContextB, List.of(record2))));

    listener.onMessage(records);

    verify(processor).consume(sliceContextA, List.of(record1));
    verify(processor).consume(sliceContextB, List.of(record2));
  }

  @Test
  public void testOnMessagePreservesConsumerRecordWrappers() {
    ConsumerRecord<String, GenericRecord> record = mock(ConsumerRecord.class);
    List<ConsumerRecord<String, GenericRecord>> records = List.of(record);

    when(batchAffinityResolver.partition(records, CONSUMER_GROUP, systemOperationContext))
        .thenReturn(
            List.of(new InboundBatchAffinityResolver.Slice<>(systemOperationContext, records)));

    listener.onMessage(records);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ConsumerRecord<String, GenericRecord>>> recordsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(processor).consume(eq(systemOperationContext), recordsCaptor.capture());
    assertEquals(recordsCaptor.getValue().size(), 1);
    assertEquals(recordsCaptor.getValue().get(0), record);
  }

  @Test
  public void testOnMessageWithDefaultAffinityResolver() {
    batchAffinityResolver = new DefaultInboundBatchAffinityResolver();
    listener =
        new MCPBatchKafkaListener(
            processor, batchAffinityResolver, systemOperationContext, CONSUMER_GROUP);

    ConsumerRecord<String, GenericRecord> record = mock(ConsumerRecord.class);
    List<ConsumerRecord<String, GenericRecord>> records = List.of(record);

    listener.onMessage(records);

    verify(processor).consume(systemOperationContext, records);
  }
}
