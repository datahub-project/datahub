package com.linkedin.gms.factory.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabled;
import io.datahubproject.event.ExternalEventsPollHandler;
import io.datahubproject.event.KafkaExternalEventsPollHandler;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@KafkaMessagingEnabled
@ConditionalOnProperty(name = "eventsApi.enabled", havingValue = "true")
public class KafkaExternalEventsPollHandlerConfiguration {

  @Bean
  public ExternalEventsPollHandler kafkaExternalEventsPollHandler(
      KafkaConsumerPool kafkaConsumerPool, ObjectMapper objectMapper) {
    return new KafkaExternalEventsPollHandler(kafkaConsumerPool, objectMapper);
  }
}
