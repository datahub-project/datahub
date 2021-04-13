package com.linkedin.gms.factory.ml;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.mxe.TopicConvention;
import javax.annotation.Nonnull;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.metadata.aspect.MLModelAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.MLModelSnapshot;

import io.ebean.config.ServerConfig;


@Configuration
public class MLModelDAOFactory {
  @Autowired
  private ApplicationContext applicationContext;

  @Bean(name = "mlModelDAO")
  @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN})
  @Nonnull
  protected EbeanLocalDAO<MLModelAspect, MLModelUrn> createInstance() {
    KafkaMetadataEventProducer<MLModelSnapshot, MLModelAspect, MLModelUrn> producer =
        new KafkaMetadataEventProducer<>(MLModelSnapshot.class, MLModelAspect.class,
            applicationContext.getBean(Producer.class), applicationContext.getBean(TopicConvention.class));
    return new EbeanLocalDAO<>(MLModelAspect.class, producer, applicationContext.getBean(ServerConfig.class),
        MLModelUrn.class);
  }
}