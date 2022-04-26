package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.mxe.TopicConvention;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@EnableConfigurationProperties(KafkaProperties.class)
@Import({DataHubKafkaProducerFactory.class, TopicConventionFactory.class})
public class DataHubKafkaEventProducerFactory {

  @Autowired
  @Qualifier("kafkaProducer")
  private Producer<String, ? extends IndexedRecord> kafkaProducer;

  @Autowired
  @Qualifier(TopicConventionFactory.TOPIC_CONVENTION_BEAN)
  private TopicConvention topicConvention;

  @Bean(name = "kafkaEventProducer")
  protected KafkaEventProducer createInstance() {
    return new KafkaEventProducer(
            kafkaProducer,
            topicConvention);
  }
}
