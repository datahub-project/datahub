package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.metadata.dao.producer.EntityKafkaMetadataEventProducer;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanEntityService;
import com.linkedin.metadata.entity.datastax.DatastaxAspectDao;
import com.linkedin.metadata.entity.datastax.DatastaxEntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.TopicConvention;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
 

@Configuration
public class EntityServiceFactory {

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "entityService")
  @DependsOn({"datastaxAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @ConditionalOnProperty(name="DAO_SERVICE_LAYER", havingValue="datastax")
  @Nonnull
  protected EntityService createDatastaxInstance() {

    System.out.println("creating datastax");
    final EntityKafkaMetadataEventProducer producer =
            new EntityKafkaMetadataEventProducer(applicationContext.getBean(Producer.class),
                    applicationContext.getBean(TopicConvention.class));

    return new DatastaxEntityService(applicationContext.getBean(DatastaxAspectDao.class), producer, applicationContext.getBean(EntityRegistry.class));
  }

  @Bean(name = "entityService")
  @DependsOn({"ebeanAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @ConditionalOnProperty(name="DAO_SERVICE_LAYER", havingValue="ebean", matchIfMissing = true)
  @Nonnull
  protected EntityService createEbeanInstance() {

    System.out.println("creating ebean");
    final EntityKafkaMetadataEventProducer producer =
        new EntityKafkaMetadataEventProducer(applicationContext.getBean(Producer.class),
            applicationContext.getBean(TopicConvention.class));

    return new EbeanEntityService(applicationContext.getBean(EbeanAspectDao.class), producer, applicationContext.getBean(EntityRegistry.class));
  }
}
