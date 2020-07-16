package com.linkedin.metadata.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;


@Slf4j
@Configuration
public class KafkaConfig {

  @Bean
  public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(KafkaProperties properties){
    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties()));
    log.info("KafkaListenerContainerFactory built successfully");
    return factory;
  }

  @Bean
  public KafkaTemplate<String, GenericRecord> kafkaTemplate(KafkaProperties properties) {
    KafkaTemplate<String, GenericRecord>  template = new KafkaTemplate<>(
        new DefaultKafkaProducerFactory<>(properties.buildProducerProperties()));
    log.info("KafkaTemplate built successfully");
    return template;
  }

}
