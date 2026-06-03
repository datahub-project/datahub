package com.linkedin.datahub.upgrade.config;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.DataHubKafkaProducerFactory;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabledCondition;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.TopicConvention;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Slf4j
@Configuration
@Conditional(KafkaMessagingEnabledCondition.class)
public class SystemUpdateKafkaMessagingConfig {

  @Autowired
  @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN)
  private TopicConvention topicConvention;

  @Autowired private KafkaHealthChecker kafkaHealthChecker;

  @Bean(name = "duheKafkaEventProducer")
  protected KafkaEventProducer duheKafkaEventProducer(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      KafkaProperties properties,
      @Qualifier("duheSchemaRegistryConfig")
          KafkaConfiguration.SerDeKeyValueConfig duheSchemaRegistryConfig,
      MetricUtils metricUtils) {
    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    Producer<String, IndexedRecord> producer =
        new KafkaProducer<>(
            DataHubKafkaProducerFactory.buildProducerProperties(
                duheSchemaRegistryConfig, kafkaConfiguration, properties));
    return new KafkaEventProducer(producer, topicConvention, kafkaHealthChecker, metricUtils);
  }

  /**
   * The ReindexDataJobViaNodesCLLConfig step requires publishing to MCL. Overriding the default
   * producer with this special producer which doesn't require an active registry.
   *
   * <p>Use when INTERNAL registry and is SYSTEM_UPDATE
   *
   * <p>This forces this producer into the EntityService
   */
  @Primary
  @Bean(name = "kafkaEventProducer")
  @ConditionalOnProperty(
      name = "kafka.schemaRegistry.type",
      havingValue = InternalSchemaRegistryFactory.TYPE)
  protected KafkaEventProducer kafkaEventProducer(
      @Qualifier("duheKafkaEventProducer") KafkaEventProducer kafkaEventProducer) {
    return kafkaEventProducer;
  }
}
