package com.linkedin.metadata.kafka.batch;

import com.linkedin.metadata.kafka.context.inbound.InboundBatchAffinityResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchMessageListener;

/**
 * Batch Kafka listener for MCP that bypasses Spring's {@code BatchMessagingMessageListenerAdapter}
 * by being wired directly via {@link
 * com.linkedin.metadata.kafka.listener.BatchKafkaListenerEndpoint}.
 */
public class MCPBatchKafkaListener implements BatchMessageListener<String, GenericRecord> {

  private final BatchMetadataChangeProposalsProcessor processor;
  private final InboundBatchAffinityResolver batchAffinityResolver;
  private final OperationContext systemOperationContext;
  private final String consumerGroupId;

  public MCPBatchKafkaListener(
      @Nonnull BatchMetadataChangeProposalsProcessor processor,
      @Nonnull InboundBatchAffinityResolver batchAffinityResolver,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull String consumerGroupId) {
    this.processor = processor;
    this.batchAffinityResolver = batchAffinityResolver;
    this.systemOperationContext = systemOperationContext;
    this.consumerGroupId = consumerGroupId;
  }

  @Override
  public void onMessage(@Nonnull List<ConsumerRecord<String, GenericRecord>> consumerRecords) {
    if (consumerRecords.isEmpty()) {
      return;
    }
    final List<InboundBatchAffinityResolver.Slice<GenericRecord>> slices =
        batchAffinityResolver.partition(consumerRecords, consumerGroupId, systemOperationContext);
    for (InboundBatchAffinityResolver.Slice<GenericRecord> slice : slices) {
      processor.consume(slice.context(), slice.records());
    }
  }
}
