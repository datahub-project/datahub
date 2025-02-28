package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.mxe.TopicConvention;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
  protected KafkaEventProducer createInstance() {
    return new KafkaEventProducer(kafkaProducer, topicConvention, kafkaHealthChecker);
  }
}
