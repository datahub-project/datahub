package com.datahub.event;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.PE_EVENT_CONSUMER_NAME;

import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.mxe.Topics;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka transport entrypoint for {@link PlatformEventProcessor}. */
@Component
@KafkaMessagingEnabled
@Conditional(PlatformEventProcessorCondition.class)
@EnableKafka
@RequiredArgsConstructor
public class PlatformEventKafkaListener {

  private final PlatformEventProcessor processor;

  @KafkaListener(
      id = PlatformEventProcessor.DATAHUB_PLATFORM_EVENT_CONSUMER_GROUP_VALUE,
      topics = {"${PLATFORM_EVENT_TOPIC_NAME:" + Topics.PLATFORM_EVENT + "}"},
      containerFactory = PE_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, GenericRecord> consumerRecord) {
    processor.consume(consumerRecord);
  }
}
