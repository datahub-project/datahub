package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.TopicConvention;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class EntityServiceFactory {

  @Bean(name = "entityService")
  @DependsOn({"entityAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @Nonnull
  protected EntityService createInstance(
      Producer<String, ? extends IndexedRecord> producer,
      TopicConvention convention,
      @Qualifier("entityAspectDao") AspectDao aspectDao,
      EntityRegistry entityRegistry) {

    final KafkaEventProducer eventProducer = new KafkaEventProducer(producer, convention);
    return new EntityService(aspectDao, eventProducer, entityRegistry);
  }
}
