package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.DEFAULT_EVENT_CONSUMER_NAME;

import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.kafka.config.MetadataChangeEventsProcessorCondition;
import com.linkedin.mxe.Topics;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka transport entrypoint for {@link MetadataChangeEventsProcessor} (pgQueue uses {@link
 * com.linkedin.metadata.pgqueue.PgQueuePollWorker}).
 */
@Component
@KafkaMessagingEnabled
@Conditional(MetadataChangeEventsProcessorCondition.class)
@EnableKafka
@RequiredArgsConstructor
public class MetadataChangeEventsKafkaListener {

  private final MetadataChangeEventsProcessor processor;

  @KafkaListener(
      id = "${METADATA_CHANGE_EVENT_KAFKA_CONSUMER_GROUP_ID:mce-consumer-job-client}",
      topics =
          "${METADATA_CHANGE_EVENT_NAME:${KAFKA_MCE_TOPIC_NAME:"
              + Topics.METADATA_CHANGE_EVENT
              + "}}",
      containerFactory = DEFAULT_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    processor.consume(consumerRecord);
  }
}
