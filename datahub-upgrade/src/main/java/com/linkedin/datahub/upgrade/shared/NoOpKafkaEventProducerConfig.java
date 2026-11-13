package com.linkedin.datahub.upgrade.shared;

import com.linkedin.metadata.event.EventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Supplies {@code kafkaEventProducer} for upgrade contexts that do not scan the Kafka factories
 * (Cleanup, LoadIndices). EntityServiceFactory depends on that bean by name, so without it those
 * contexts fail to start. Import this from any upgrade context that never emits events.
 */
@Slf4j
@Configuration
public class NoOpKafkaEventProducerConfig {

  @Bean(name = "kafkaEventProducer")
  public EventProducer kafkaEventProducer() {
    log.info("Event emission is disabled for this upgrade: using NoOpKafkaEventProducer");
    return new NoOpKafkaEventProducer();
  }
}
