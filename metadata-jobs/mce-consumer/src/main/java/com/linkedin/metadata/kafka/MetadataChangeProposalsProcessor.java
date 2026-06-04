package com.linkedin.metadata.kafka;

import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.kafka.config.MetadataChangeProposalProcessorCondition;
import com.linkedin.metadata.kafka.pause.ConsumerPauseSupport;
import com.linkedin.metadata.kafka.util.KafkaListenerUtil;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * Kafka transport entrypoint for single MCP processing via {@link MetadataChangeProposalConsumer}.
 * Follow-up: align remaining split-path processors on {@link InboundMetadataEnvelope} (see PE
 * {@code consumeEnvelope} pattern).
 */
@Slf4j
@Component
@Import({RestliEntityClientFactory.class})
@Conditional(MetadataChangeProposalProcessorCondition.class)
@RequiredArgsConstructor
public class MetadataChangeProposalsProcessor {

  @Qualifier("kafkaThrottle")
  private final ThrottleSensor kafkaThrottle;

  private final ConfigurationProvider provider;
  private final ConsumerPauseSupport consumerPauseSupport;
  private final MetadataChangeProposalConsumer metadataChangeProposalConsumer;

  @Value(MCP_CONSUMER_GROUP_ID_VALUE)
  private String mceConsumerGroupId;

  @PostConstruct
  public void registerConsumerThrottle() {
    KafkaListenerUtil.registerThrottle(
        kafkaThrottle, provider, consumerPauseSupport, mceConsumerGroupId);
  }

  /** Used by {@link MetadataChangeProposalsKafkaListener} and tests. */
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    metadataChangeProposalConsumer.accept(
        InboundMetadataEnvelope.fromKafka(consumerRecord, mceConsumerGroupId), mceConsumerGroupId);
  }
}
