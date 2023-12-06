package com.linkedin.gms.factory.entity;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.mxe.TopicConvention;
import javax.annotation.Nonnull;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class EntityServiceFactory {

  @Value("${EBEAN_MAX_TRANSACTION_RETRY:#{null}}")
  private Integer _ebeanMaxTransactionRetry;

  @Bean(name = "entityService")
  @DependsOn({
    "entityAspectDao",
    "kafkaEventProducer",
    "kafkaHealthChecker",
    TopicConventionFactory.TOPIC_CONVENTION_BEAN,
    "entityRegistry"
  })
  @Nonnull
  protected EntityService createInstance(
      Producer<String, ? extends IndexedRecord> producer,
      TopicConvention convention,
      KafkaHealthChecker kafkaHealthChecker,
      @Qualifier("entityAspectDao") AspectDao aspectDao,
      EntityRegistry entityRegistry,
      ConfigurationProvider configurationProvider,
      UpdateIndicesService updateIndicesService) {

    final KafkaEventProducer eventProducer =
        new KafkaEventProducer(producer, convention, kafkaHealthChecker);
    FeatureFlags featureFlags = configurationProvider.getFeatureFlags();
    EntityService entityService =
        new EntityServiceImpl(
            aspectDao,
            eventProducer,
            entityRegistry,
            featureFlags.isAlwaysEmitChangeLog(),
            updateIndicesService,
            featureFlags.getPreProcessHooks(),
            _ebeanMaxTransactionRetry);

    return entityService;
  }
}
