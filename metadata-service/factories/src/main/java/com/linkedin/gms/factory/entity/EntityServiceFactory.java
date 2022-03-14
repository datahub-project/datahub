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
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;
 

@Configuration
public class EntityServiceFactory {

  @Bean(name = "entityService")
  @DependsOn({"datastaxAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @ConditionalOnProperty(name = "ENTITY_SERVICE_IMPL", havingValue = "datastax")
  @Nonnull
  protected EntityService createDatastaxInstance(
      Producer<String, ? extends IndexedRecord> producer,
      TopicConvention convention,
      DatastaxAspectDao aspectDao,
      EntityRegistry entityRegistry) {

    final EntityKafkaMetadataEventProducer metadataProducer = new EntityKafkaMetadataEventProducer(producer, convention);
    return new DatastaxEntityService(aspectDao, metadataProducer, entityRegistry);
  }

  @Bean(name = "entityService")
  @DependsOn({"ebeanAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @ConditionalOnProperty(name = "ENTITY_SERVICE_IMPL", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected EntityService createEbeanInstance(
      Producer<String, ? extends IndexedRecord> producer,
      TopicConvention convention,
      EbeanAspectDao aspectDao,
      EntityRegistry entityRegistry) {

    final EntityKafkaMetadataEventProducer metadataProducer = new EntityKafkaMetadataEventProducer(producer, convention);
    return new EbeanEntityService(aspectDao, metadataProducer, entityRegistry);
  }
}
