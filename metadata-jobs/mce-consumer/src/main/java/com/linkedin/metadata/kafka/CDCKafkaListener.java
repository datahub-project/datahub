package com.linkedin.metadata.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.CDC_EVENT_CONSUMER_NAME;

import com.linkedin.gms.factory.entityclient.RestliEntityClientFactory;
import com.linkedin.gms.factory.kafka.CDCConsumerFactory;
import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import com.linkedin.metadata.kafka.config.CDCProcessorCondition;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka transport entrypoint for {@link CDCProcessor}. */
@Component
@KafkaMessagingEnabled
@Conditional(CDCProcessorCondition.class)
@EnableKafka
@Import({
  RestliEntityClientFactory.class,
  SimpleKafkaConsumerFactory.class,
  CDCConsumerFactory.class
})
@RequiredArgsConstructor
public class CDCKafkaListener {

  private final CDCProcessor processor;

  @KafkaListener(
      id = CDCProcessor.CDC_CONSUMER_GROUP_ID,
      topics =
          "#{${mclProcessing.cdcSource.enabled:true}?'${kafka.topics.cdcTopic.name:datahub.datahub.metadata_aspect_v2}' : null}",
      containerFactory = CDC_EVENT_CONSUMER_NAME,
      autoStartup = "false")
  public void consume(final ConsumerRecord<String, String> consumerRecord) {
    processor.consumeKafka(consumerRecord);
  }
}
