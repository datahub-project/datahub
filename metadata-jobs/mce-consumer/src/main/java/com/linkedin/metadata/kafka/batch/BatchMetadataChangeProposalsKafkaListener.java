package com.linkedin.metadata.kafka.batch;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_EVENT_CONSUMER_NAME;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.kafka.context.inbound.InboundBatchAffinityResolver;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka transport entrypoint for batch MCP. Partitions the inbound batch into affinity slices via
 * {@link InboundBatchAffinityResolver} (single OSS slice by default; deployments that register
 * their own {@link InboundBatchAffinityResolver} bean shadow the default via
 * {@code @ConditionalOnMissingBean} on the registration site, getting one slice per affinity group)
 * and dispatches each slice independently through {@link
 * BatchMetadataChangeProposalsProcessor#consume(OperationContext, List)}.
 */
@Component
@KafkaMessagingEnabled
@Import({RestliEntityClientFactory.class})
@Conditional(BatchMetadataChangeProposalProcessorCondition.class)
@EnableKafka
public class BatchMetadataChangeProposalsKafkaListener {

  private final BatchMetadataChangeProposalsProcessor processor;
  private final InboundBatchAffinityResolver batchAffinityResolver;
  private final OperationContext systemOperationContext;

  @Autowired
  public BatchMetadataChangeProposalsKafkaListener(
      @Nonnull final BatchMetadataChangeProposalsProcessor processor,
      @Nonnull final InboundBatchAffinityResolver batchAffinityResolver,
      @Qualifier("systemOperationContext") @Nonnull final OperationContext systemOperationContext) {
    this.processor = processor;
    this.batchAffinityResolver = batchAffinityResolver;
    this.systemOperationContext = systemOperationContext;
  }

  @KafkaListener(
      id = MCP_CONSUMER_GROUP_ID_VALUE,
      topics = "${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}",
      containerFactory = MCP_EVENT_CONSUMER_NAME,
      batch = "true",
      autoStartup = "false")
  public void consume(final List<ConsumerRecord<String, GenericRecord>> consumerRecords) {
    if (consumerRecords.isEmpty()) {
      return;
    }
    // Compute partitions once; the @ConditionalOnMissingBean OSS default returns a single slice.
    final List<InboundBatchAffinityResolver.Slice<GenericRecord>> slices =
        batchAffinityResolver.partition(
            consumerRecords, MCP_CONSUMER_GROUP_ID_VALUE, systemOperationContext);
    for (InboundBatchAffinityResolver.Slice<GenericRecord> slice : slices) {
      processor.consume(slice.context(), slice.records());
    }
  }
}
