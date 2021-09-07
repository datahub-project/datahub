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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
 

@Configuration
public class EntityServiceFactory {

  @Value("${DAO_SERVICE_LAYER:ebean}")
  private String daoServiceLayer;

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "entityService")
  @DependsOn({"datastaxAspectDao", "ebeanAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @Nonnull
  protected EntityService createInstance() {

    final EntityKafkaMetadataEventProducer producer =
        new EntityKafkaMetadataEventProducer(applicationContext.getBean(Producer.class),
            applicationContext.getBean(TopicConvention.class));

    switch (daoServiceLayer) {
      case "datastax":
        return new DatastaxEntityService(applicationContext.getBean(DatastaxAspectDao.class), producer,
                                         applicationContext.getBean(EntityRegistry.class));
      
    default: // ebean
        return new EbeanEntityService(applicationContext.getBean(EbeanAspectDao.class), producer,
        applicationContext.getBean(EntityRegistry.class));
    }
  }
}
