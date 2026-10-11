package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_EVENT_CONSUMER_NAME;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.kafka.config.MetadataChangeProposalProcessorCondition;
import com.linkedin.mxe.Topics;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka transport entrypoint for MCP ({@link MetadataChangeProposalsProcessor}). */
@Component
@KafkaMessagingEnabled
@Import({RestliEntityClientFactory.class})
@Conditional(MetadataChangeProposalProcessorCondition.class)
@EnableKafka
@RequiredArgsConstructor
public class MetadataChangeProposalsKafkaListener {

  private final MetadataChangeProposalsProcessor processor;

  @KafkaListener(
      id = MCP_CONSUMER_GROUP_ID_VALUE,
      topics = "${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}",
      containerFactory = MCP_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    processor.consume(consumerRecord);
  }
}
