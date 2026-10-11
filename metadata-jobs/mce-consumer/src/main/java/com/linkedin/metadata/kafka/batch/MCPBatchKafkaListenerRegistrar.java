package com.linkedin.metadata.kafka.batch;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_BATCH_EVENT_CONSUMER_NAME;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.kafka.context.inbound.InboundBatchAffinityResolver;
import com.linkedin.metadata.kafka.listener.BatchKafkaListenerEndpoint;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

/**
 * Registers the MCP batch Kafka consumer programmatically via {@link BatchKafkaListenerEndpoint},
 * bypassing {@code @KafkaListener} method invocation that breaks under Spring Kafka 4.x batch mode.
 */
@Slf4j
@Component
@KafkaMessagingEnabled
@Import({RestliEntityClientFactory.class})
@Conditional(BatchMetadataChangeProposalProcessorCondition.class)
public class MCPBatchKafkaListenerRegistrar implements InitializingBean {

  private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  private final KafkaListenerContainerFactory<?> batchKafkaListenerContainerFactory;
  private final BatchMetadataChangeProposalsProcessor processor;
  private final InboundBatchAffinityResolver batchAffinityResolver;
  private final OperationContext systemOperationContext;

  @Value(MCP_CONSUMER_GROUP_ID_VALUE)
  private String consumerGroupId;

  @Value("${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}")
  private String mcpTopicName;

  @Autowired
  public MCPBatchKafkaListenerRegistrar(
      KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
      @Qualifier(MCP_BATCH_EVENT_CONSUMER_NAME)
          KafkaListenerContainerFactory<?> batchKafkaListenerContainerFactory,
      @Nonnull BatchMetadataChangeProposalsProcessor processor,
      @Nonnull InboundBatchAffinityResolver batchAffinityResolver,
      @Qualifier("systemOperationContext") @Nonnull OperationContext systemOperationContext) {
    this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    this.batchKafkaListenerContainerFactory = batchKafkaListenerContainerFactory;
    this.processor = processor;
    this.batchAffinityResolver = batchAffinityResolver;
    this.systemOperationContext = systemOperationContext;
  }

  @Override
  public void afterPropertiesSet() {
    MCPBatchKafkaListener listener =
        new MCPBatchKafkaListener(
            processor, batchAffinityResolver, systemOperationContext, consumerGroupId);

    BatchKafkaListenerEndpoint<String, GenericRecord> endpoint =
        new BatchKafkaListenerEndpoint<>(
            consumerGroupId, consumerGroupId, List.of(mcpTopicName), listener::onMessage);

    kafkaListenerEndpointRegistry.registerListenerContainer(
        endpoint, batchKafkaListenerContainerFactory, false);

    log.info(
        "Registered MCP batch Kafka listener for topic {} with consumer group {}",
        mcpTopicName,
        consumerGroupId);
  }
}
