package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.metadata.dao.producer.EntityKafkaMetadataEventProducer;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.datastax.DatastaxAspectDao;
import com.linkedin.metadata.entity.datastax.DatastaxEntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanEntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.TopicConvention;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;
 

@Configuration
public class EntityServiceFactory {

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "entityService")
  @DependsOn({"datastaxAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @ConditionalOnProperty(name = "ENTITY_SERVICE_IMPL", havingValue = "datastax")
  @Nonnull
  protected EntityService createDatastaxInstance() {

    final EntityKafkaMetadataEventProducer producer =
        new EntityKafkaMetadataEventProducer(applicationContext.getBean(Producer.class),
            applicationContext.getBean(TopicConvention.class));

    return new DatastaxEntityService(applicationContext.getBean(DatastaxAspectDao.class), producer, applicationContext.getBean(EntityRegistry.class));
  }

  @Bean(name = "entityService")
  @DependsOn({"ebeanAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @ConditionalOnProperty(name = "ENTITY_SERVICE_IMPL", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected EntityService createEbeanInstance() {

    final EntityKafkaMetadataEventProducer producer =
        new EntityKafkaMetadataEventProducer(applicationContext.getBean(Producer.class),
            applicationContext.getBean(TopicConvention.class));

    return new EbeanEntityService(applicationContext.getBean(EbeanAspectDao.class), producer, applicationContext.getBean(EntityRegistry.class));
  }
}
