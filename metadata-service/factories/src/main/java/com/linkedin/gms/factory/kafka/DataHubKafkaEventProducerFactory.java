package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.dao.producer.GenericProducerImpl;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.event.GenericProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.TopicConvention;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataHubKafkaEventProducerFactory {

  @Autowired
  @Qualifier("kafkaProducer")
  private Producer<String, ? extends IndexedRecord> kafkaProducer;

  @Autowired
  @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN)
  private TopicConvention topicConvention;

  @Autowired private KafkaHealthChecker kafkaHealthChecker;

  @Bean(name = "kafkaEventProducer")
  protected KafkaEventProducer createInstance(
      MetricUtils metricUtils, ConfigurationProvider configurationProvider) {
    KafkaEventProducer kafkaEventProducer =
        new KafkaEventProducer(kafkaProducer, topicConvention, kafkaHealthChecker, metricUtils);
    if (configurationProvider.getDatahub().isReadOnly()) {
      kafkaEventProducer.setWritable(false);
    }

    return kafkaEventProducer;
  }

  @Bean(name = "dataHubUsageEventProducer")
  @ConditionalOnBean(name = "dataHubUsageProducer")
  protected GenericProducer<String> createUsageInstance(
      @Qualifier("dataHubUsageProducer") Producer<String, String> usageProducer,
      MetricUtils metricUtils,
      ConfigurationProvider configurationProvider) {
    GenericProducer<String> genericProducer =
        new GenericProducerImpl<>(usageProducer, kafkaHealthChecker, metricUtils);
    if (configurationProvider.getDatahub().isReadOnly()) {
      genericProducer.setWritable(false);
    }
    return genericProducer;
  }
}
