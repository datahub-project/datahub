package com.linkedin.metadata.kafka.batch;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_EVENT_CONSUMER_NAME;
import static com.linkedin.mxe.ConsumerGroups.MCP_CONSUMER_GROUP_ID_VALUE;

import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import com.linkedin.mxe.Topics;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka transport entrypoint for batch MCP ({@link BatchMetadataChangeProposalsProcessor}). */
@Component
@KafkaMessagingEnabled
@Import({RestliEntityClientFactory.class})
@Conditional(BatchMetadataChangeProposalProcessorCondition.class)
@EnableKafka
@RequiredArgsConstructor
public class BatchMetadataChangeProposalsKafkaListener {

  private final BatchMetadataChangeProposalsProcessor processor;

  @KafkaListener(
      id = MCP_CONSUMER_GROUP_ID_VALUE,
      topics = "${METADATA_CHANGE_PROPOSAL_TOPIC_NAME:" + Topics.METADATA_CHANGE_PROPOSAL + "}",
      containerFactory = MCP_EVENT_CONSUMER_NAME,
      batch = "true",
      autoStartup = "false")
  public void consume(final List<ConsumerRecord<String, GenericRecord>> consumerRecords) {
    processor.consume(consumerRecords);
  }
}
