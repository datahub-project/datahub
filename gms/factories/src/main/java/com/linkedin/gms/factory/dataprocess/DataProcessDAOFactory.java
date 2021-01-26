package com.linkedin.gms.factory.dataprocess;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.metadata.aspect.DataProcessAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.dao.producer.KafkaProducerCallback;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import com.linkedin.mxe.TopicConvention;
import io.ebean.config.ServerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
@ComponentScan(basePackages = "com.linkedin")
public class DataProcessDAOFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "dataProcessDAO")
  @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN})
  protected BaseLocalDAO<DataProcessAspect, DataProcessUrn> createInstance() {
    KafkaMetadataEventProducer<DataProcessSnapshot, DataProcessAspect, DataProcessUrn> producer =
        new KafkaMetadataEventProducer(DataProcessSnapshot.class, DataProcessAspect.class,
            applicationContext.getBean(Producer.class), applicationContext.getBean(TopicConvention.class),
            new KafkaProducerCallback());

    return new EbeanLocalDAO<>(DataProcessAspect.class, producer, applicationContext.getBean(ServerConfig.class),
        DataProcessUrn.class);
  }
}
