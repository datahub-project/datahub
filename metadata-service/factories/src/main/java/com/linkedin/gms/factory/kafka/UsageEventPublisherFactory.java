package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabledCondition;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.dao.producer.KafkaUsageEventPublisher;
import com.linkedin.metadata.event.UsageEventPublisher;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(KafkaMessagingEnabledCondition.class)
public class UsageEventPublisherFactory {

  @Autowired private KafkaHealthChecker kafkaHealthChecker;

  /**
   * Legacy bean name retained for injection sites that still qualify {@code
   * dataHubUsageEventProducer}.
   */
  @Bean(name = {"usageEventPublisher", "dataHubUsageEventProducer"})
  protected UsageEventPublisher usageEventPublisher(
      @Qualifier("dataHubUsageProducer") Producer<String, String> usageProducer,
      MetricUtils metricUtils,
      ConfigurationProvider configurationProvider) {
    KafkaUsageEventPublisher publisher =
        new KafkaUsageEventPublisher(usageProducer, kafkaHealthChecker, metricUtils);
    if (configurationProvider.getDatahub().isReadOnly()) {
      publisher.setWritable(false);
    }
    return publisher;
  }
}
