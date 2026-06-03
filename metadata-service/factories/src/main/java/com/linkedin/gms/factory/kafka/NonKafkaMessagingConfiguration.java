package com.linkedin.gms.factory.kafka;

import com.linkedin.metadata.config.messaging.NonKafkaNonPgQueueMessagingTransportCondition;
import com.linkedin.metadata.event.NoOpUsageEventPublisher;
import com.linkedin.metadata.event.UsageEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Stand-ins when messaging transport is neither Kafka nor pgQueue. For pgQueue, usage events use
 * {@link PgQueueUsageEventPublisherConfiguration}; for Kafka, {@link UsageEventPublisherFactory}.
 */
@Configuration
@Conditional(NonKafkaNonPgQueueMessagingTransportCondition.class)
public class NonKafkaMessagingConfiguration {

  @Bean(name = {"usageEventPublisher", "dataHubUsageEventProducer"})
  protected UsageEventPublisher usageEventPublisher() {
    return new NoOpUsageEventPublisher();
  }
}
