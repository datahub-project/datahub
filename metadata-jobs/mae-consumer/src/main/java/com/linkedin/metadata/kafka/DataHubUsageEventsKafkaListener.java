package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.SIMPLE_EVENT_CONSUMER_NAME;

import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.kafka.config.DataHubUsageEventsProcessorCondition;
import com.linkedin.mxe.Topics;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka transport entrypoint for {@link DataHubUsageEventsProcessor} (pgQueue uses {@link
 * com.linkedin.metadata.kafka.config.PgQueueMaePollerSourcesConfiguration}).
 */
@Component
@KafkaMessagingEnabled
@Conditional(DataHubUsageEventsProcessorCondition.class)
@Import({SimpleKafkaConsumerFactory.class})
@EnableKafka
@RequiredArgsConstructor
public class DataHubUsageEventsKafkaListener {

  private final DataHubUsageEventsProcessor processor;

  @KafkaListener(
      id = DataHubUsageEventsProcessor.DATAHUB_USAGE_EVENT_KAFKA_CONSUMER_GROUP_VALUE,
      topics = "${DATAHUB_USAGE_EVENT_NAME:" + Topics.DATAHUB_USAGE_EVENT + "}",
      containerFactory = SIMPLE_EVENT_CONSUMER_NAME,
      batch = "true",
      autoStartup = "false")
  public void consume(final List<ConsumerRecord<String, String>> consumerRecords) {
    processor.consume(consumerRecords);
  }
}
