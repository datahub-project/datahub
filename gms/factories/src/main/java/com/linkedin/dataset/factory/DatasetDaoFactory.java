package com.linkedin.dataset.factory;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.dao.producer.KafkaProducerCallback;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import io.ebean.config.ServerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class DatasetDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "datasetDao")
  @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer"})
  protected EbeanLocalDAO<DatasetAspect, DatasetUrn> createInstance() {
    KafkaMetadataEventProducer<DatasetSnapshot, DatasetAspect, DatasetUrn> producer =
        new KafkaMetadataEventProducer(DatasetSnapshot.class,
                DatasetAspect.class,
                applicationContext.getBean(Producer.class),
                new KafkaProducerCallback());

    return new EbeanLocalDAO<>(DatasetAspect.class, producer, applicationContext.getBean(ServerConfig.class));
  }
}
