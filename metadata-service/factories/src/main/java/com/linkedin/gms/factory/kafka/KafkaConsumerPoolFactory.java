package com.linkedin.gms.factory.kafka;

import io.acryl.event.kafka.KafkaConsumerPool;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

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
