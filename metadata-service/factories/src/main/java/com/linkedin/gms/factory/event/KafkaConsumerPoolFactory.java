package com.linkedin.gms.factory.event;

import io.datahubproject.event.kafka.KafkaConsumerPool;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@ConditionalOnProperty(name = "eventsApi.enabled", havingValue = "true")
@Configuration
public class KafkaConsumerPoolFactory {
  @Value("${kafka.consumerPool.initialSize:5}")
  private int initialPoolSize;

  @Value("${kafka.consumerPool.maxSize:10}")
  private int maxPoolSize;

  @Bean
  public KafkaConsumerPool kafkaConsumerPool(
      @Qualifier("kafkaConsumerPoolConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> consumerFactory) {
    return new KafkaConsumerPool(consumerFactory, initialPoolSize, maxPoolSize);
  }
}
