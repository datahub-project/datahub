package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;
import com.linkedin.metadata.entity.cassandra.CassandraEntityService;
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
  @DependsOn({"cassandraAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected EntityService createCassandraInstance(
      Producer<String, ? extends IndexedRecord> producer,
      TopicConvention convention,
      CassandraAspectDao aspectDao,
      EntityRegistry entityRegistry) {

    final KafkaEventProducer eventProducer = new KafkaEventProducer(producer, convention);
    return new CassandraEntityService(aspectDao, eventProducer, entityRegistry);
  }

  @Bean(name = "entityService")
  @DependsOn({"ebeanAspectDao", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN, "entityRegistry"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected EntityService createEbeanInstance(
      Producer<String, ? extends IndexedRecord> producer,
      TopicConvention convention,
      EbeanAspectDao aspectDao,
      EntityRegistry entityRegistry) {

    final KafkaEventProducer eventProducer = new KafkaEventProducer(producer, convention);
    return new EbeanEntityService(aspectDao, eventProducer, entityRegistry);
  }
}
